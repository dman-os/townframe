use crate::interlude::*;

use crate::ffi::FfiError;

#[derive(Clone, Debug, uniffi::Record)]
pub struct CameraDeviceInfo {
    pub device_id: u32,
    pub label: String,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum CameraPreviewFrameEncoding {
    Jpeg,
    Rgb24,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct CameraPreviewFrame {
    pub width_px: u32,
    pub height_px: u32,
    pub encoding: CameraPreviewFrameEncoding,
    pub frame_bytes: Vec<u8>,
}

#[uniffi::export(with_foreign)]
pub trait CameraPreviewFrameListener: Send + Sync + 'static {
    fn on_camera_preview_frame(&self, frame: CameraPreviewFrame);
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct CameraNormalizedRect {
    pub left: f32,
    pub top: f32,
    pub right: f32,
    pub bottom: f32,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum CameraOverlay {
    Grid,
    QrBounds {
        bounds: CameraNormalizedRect,
    },
}

#[uniffi::export(with_foreign)]
pub trait CameraQrEventListener: Send + Sync + 'static {
    fn on_camera_qr_overlays_updated(&self, overlays: Vec<CameraOverlay>);
    fn on_camera_qr_detected(&self, decoded_text: String);
    fn on_camera_qr_error(&self, message: String);
}

#[derive(uniffi::Object)]
pub struct CameraQrAnalyzerFfi {
    listener: std::sync::Mutex<Option<std::sync::Arc<dyn CameraQrEventListener>>>,
}

fn normalize_rect(
    width_px: u32,
    height_px: u32,
    corners: &[rqrr::Point; 4],
) -> Option<CameraNormalizedRect> {
    if width_px == 0 || height_px == 0 {
        return None;
    }

    let mut min_x = f32::MAX;
    let mut min_y = f32::MAX;
    let mut max_x = f32::MIN;
    let mut max_y = f32::MIN;

    for point in corners {
        let point_x = point.x as f32;
        let point_y = point.y as f32;
        min_x = min_x.min(point_x);
        min_y = min_y.min(point_y);
        max_x = max_x.max(point_x);
        max_y = max_y.max(point_y);
    }

    let width = width_px as f32;
    let height = height_px as f32;
    let mut left = (min_x / width).clamp(0.0, 1.0);
    let mut top = (min_y / height).clamp(0.0, 1.0);
    let mut right = (max_x / width).clamp(0.0, 1.0);
    let mut bottom = (max_y / height).clamp(0.0, 1.0);

    // rqrr bounds are typically around the decoded grid. Expand slightly so
    // the overlay covers the whole visible QR marker more consistently.
    let padding_scale = 0.10_f32;
    let pad_x = (right - left) * padding_scale;
    let pad_y = (bottom - top) * padding_scale;
    left = (left - pad_x).clamp(0.0, 1.0);
    top = (top - pad_y).clamp(0.0, 1.0);
    right = (right + pad_x).clamp(0.0, 1.0);
    bottom = (bottom + pad_y).clamp(0.0, 1.0);

    Some(CameraNormalizedRect {
        left,
        top,
        right,
        bottom,
    })
}

fn camera_frame_to_luma(
    encoding: &CameraPreviewFrameEncoding,
    width_px: u32,
    height_px: u32,
    frame_bytes: &[u8],
) -> Result<image::GrayImage, FfiError> {
    match encoding {
        CameraPreviewFrameEncoding::Jpeg => {
            let decoded = image::load_from_memory(frame_bytes).map_err(|error| {
                FfiError::from(eyre::eyre!("failed decoding camera frame: {error}"))
            })?;
            Ok(decoded.to_luma8())
        }
        CameraPreviewFrameEncoding::Rgb24 => {
            let rgb = image::RgbImage::from_raw(width_px, height_px, frame_bytes.to_vec())
                .ok_or_else(|| {
                    FfiError::from(eyre::eyre!(
                        "invalid rgb24 frame dimensions {}x{} for {} bytes",
                        width_px,
                        height_px,
                        frame_bytes.len()
                    ))
                })?;
            Ok(image::DynamicImage::ImageRgb8(rgb).to_luma8())
        }
    }
}

fn publish_qr_for_frame(
    listener: &std::sync::Arc<dyn CameraQrEventListener>,
    encoding: &CameraPreviewFrameEncoding,
    width_px: u32,
    height_px: u32,
    frame_bytes: &[u8],
) -> Result<(), FfiError> {
    let grayscale = camera_frame_to_luma(encoding, width_px, height_px, frame_bytes)?;
    let mut prepared = rqrr::PreparedImage::prepare(grayscale);
    let grids = prepared.detect_grids();

    let mut overlays = Vec::with_capacity(grids.len() + 1);
    overlays.push(CameraOverlay::Grid);
    for grid in &grids {
        if let Some(bounds) = normalize_rect(width_px, height_px, &grid.bounds) {
            overlays.push(CameraOverlay::QrBounds { bounds });
        }
    }
    listener.on_camera_qr_overlays_updated(overlays);

    for grid in grids {
        match grid.decode() {
            Ok((_meta, decoded_text)) => listener.on_camera_qr_detected(decoded_text),
            Err(error) => listener.on_camera_qr_error(format!("qr decode failed: {error}")),
        }
    }
    Ok(())
}

#[uniffi::export]
impl CameraQrAnalyzerFfi {
    #[uniffi::constructor]
    pub fn load() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            listener: std::sync::Mutex::new(None),
        })
    }

    pub fn set_listener(&self, listener: std::sync::Arc<dyn CameraQrEventListener>) {
        let mut guard = self
            .listener
            .lock()
            .expect("camera qr listener mutex should not be poisoned");
        *guard = Some(listener);
    }

    pub fn clear_listener(&self) {
        let mut guard = self
            .listener
            .lock()
            .expect("camera qr listener mutex should not be poisoned");
        *guard = None;
    }

    pub fn submit_jpeg_frame(
        &self,
        width_px: u32,
        height_px: u32,
        frame_bytes: Vec<u8>,
    ) -> Result<(), FfiError> {
        let listener = {
            let guard = self
                .listener
                .lock()
                .expect("camera qr listener mutex should not be poisoned");
            std::sync::Arc::clone(
                guard
                    .as_ref()
                    .expect("CameraQrAnalyzerFfi listener must be set before submitting frames"),
            )
        };

        publish_qr_for_frame(
            &listener,
            &CameraPreviewFrameEncoding::Jpeg,
            width_px,
            height_px,
            &frame_bytes,
        )
    }
}
#[derive(uniffi::Object)]
pub struct CameraPreviewFfi {
    #[cfg(all(
        feature = "nokhwa",
        any(target_os = "linux", target_os = "macos", target_os = "windows")
    ))]
    state: std::sync::Mutex<DesktopCameraState>,
}

#[cfg(all(
    feature = "nokhwa",
    any(target_os = "linux", target_os = "macos", target_os = "windows")
))]
struct DesktopCameraState {
    stream: Option<nokhwa::CallbackCamera>,
    latest_frame: std::sync::Arc<std::sync::Mutex<Option<CameraPreviewFrame>>>,
    qr_listener: std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<dyn CameraQrEventListener>>>>,
    qr_enabled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    qr_last_decode_at: std::sync::Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

#[cfg(all(
    feature = "nokhwa",
    any(target_os = "linux", target_os = "macos", target_os = "windows")
))]
impl Default for DesktopCameraState {
    fn default() -> Self {
        Self {
            stream: None,
            latest_frame: std::sync::Arc::new(std::sync::Mutex::new(None)),
            qr_listener: std::sync::Arc::new(std::sync::Mutex::new(None)),
            qr_enabled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            qr_last_decode_at: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

#[cfg(all(
    feature = "nokhwa",
    any(target_os = "linux", target_os = "macos", target_os = "windows")
))]
fn camera_index_to_u32(camera_index: &nokhwa::utils::CameraIndex) -> Option<u32> {
    match camera_index {
        nokhwa::utils::CameraIndex::Index(index) => Some(*index),
        nokhwa::utils::CameraIndex::String(_) => None,
    }
}

#[cfg(all(
    feature = "nokhwa",
    any(target_os = "linux", target_os = "macos", target_os = "windows")
))]
fn query_devices() -> Result<Vec<nokhwa::utils::CameraInfo>, FfiError> {
    let backend = nokhwa::native_api_backend().ok_or_else(|| {
        FfiError::from(eyre::eyre!(
            "native camera backend is not available on this desktop platform"
        ))
    })?;

    nokhwa::query(backend)
        .map_err(|error| FfiError::from(eyre::eyre!("failed to query cameras: {error}")))
}

#[cfg(all(
    feature = "nokhwa",
    any(target_os = "linux", target_os = "macos", target_os = "windows")
))]
fn requested_format() -> nokhwa::utils::RequestedFormat<'static> {
    use nokhwa::utils::RequestedFormatType;

    nokhwa::utils::RequestedFormat::new::<nokhwa::pixel_format::RgbFormat>(
        RequestedFormatType::None,
    )
}

#[cfg(not(all(
    feature = "nokhwa",
    any(target_os = "linux", target_os = "macos", target_os = "windows")
)))]
fn panic_unsupported() -> ! {
    panic!("CameraPreviewFfi is only supported on desktop targets built with the `nokhwa` feature");
}

#[uniffi::export]
impl CameraPreviewFfi {
    #[uniffi::constructor]
    pub fn load() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            #[cfg(all(
                feature = "nokhwa",
                any(target_os = "linux", target_os = "macos", target_os = "windows")
            ))]
            state: std::sync::Mutex::new(DesktopCameraState::default()),
        })
    }

    pub fn list_devices(&self) -> Result<Vec<CameraDeviceInfo>, FfiError> {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            let devices = query_devices()?;
            let devices = devices
                .into_iter()
                .filter_map(|info| {
                    let device_id = camera_index_to_u32(info.index())?;
                    let is_supported = nokhwa::CallbackCamera::new(
                        info.index().clone(),
                        requested_format(),
                        |_buffer| {},
                    )
                    .is_ok();
                    if !is_supported {
                        return None;
                    }
                    Some(CameraDeviceInfo {
                        device_id,
                        label: format!("{} ({})", info.human_name(), info.description()),
                    })
                })
                .collect();
            Ok(devices)
        }

        #[cfg(not(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        )))]
        {
            panic_unsupported();
        }
    }

    pub fn supports_native_qr_analysis(&self) -> bool {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            true
        }
        #[cfg(not(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        )))]
        {
            false
        }
    }

    pub fn set_qr_listener(&self, listener: std::sync::Arc<dyn CameraQrEventListener>) {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            let state_guard = self
                .state
                .lock()
                .expect("camera state mutex should not be poisoned");
            let mut listener_guard = state_guard
                .qr_listener
                .lock()
                .expect("qr listener mutex should not be poisoned");
            *listener_guard = Some(listener);
        }
        #[cfg(not(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        )))]
        {
            let _ = listener;
        }
    }

    pub fn clear_qr_listener(&self) {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            let state_guard = self
                .state
                .lock()
                .expect("camera state mutex should not be poisoned");
            let mut listener_guard = state_guard
                .qr_listener
                .lock()
                .expect("qr listener mutex should not be poisoned");
            *listener_guard = None;
        }
    }

    pub fn set_qr_analysis_enabled(&self, enabled: bool) {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            let state_guard = self
                .state
                .lock()
                .expect("camera state mutex should not be poisoned");
            state_guard
                .qr_enabled
                .store(enabled, std::sync::atomic::Ordering::Release);
            if !enabled {
                let mut last_decode_guard = state_guard
                    .qr_last_decode_at
                    .lock()
                    .expect("qr decode timestamp mutex should not be poisoned");
                *last_decode_guard = None;
            }
        }
        #[cfg(not(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        )))]
        {
            let _ = enabled;
        }
    }

    pub fn start_stream(
        &self,
        device_id: u32,
        listener: std::sync::Arc<dyn CameraPreviewFrameListener>,
    ) -> Result<(), FfiError> {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            let mut state_guard = self
                .state
                .lock()
                .expect("camera state mutex should not be poisoned");

            if let Some(mut stream) = state_guard.stream.take() {
                stream.stop_stream().map_err(|error| {
                    FfiError::from(eyre::eyre!("failed stopping previous stream: {error}"))
                })?;
            }

            let mut devices = query_devices()?;
            let selected_info = devices
                .drain(..)
                .find(|info| camera_index_to_u32(info.index()) == Some(device_id))
                .ok_or_else(|| {
                    FfiError::from(eyre::eyre!(
                        "camera device_id={} not found in current device list (or not supported)",
                        device_id
                    ))
                })?;

            let latest_frame = std::sync::Arc::clone(&state_guard.latest_frame);
            let _listener = std::sync::Arc::clone(&listener);
            let qr_listener = std::sync::Arc::clone(&state_guard.qr_listener);
            let qr_enabled = std::sync::Arc::clone(&state_guard.qr_enabled);
            let qr_last_decode_at = std::sync::Arc::clone(&state_guard.qr_last_decode_at);

            let mut stream = nokhwa::CallbackCamera::new(
                selected_info.index().clone(),
                requested_format(),
                move |buffer| {
                    let resolution = buffer.resolution();
                    let frame = if buffer.source_frame_format() == nokhwa::utils::FrameFormat::MJPEG
                    {
                        CameraPreviewFrame {
                            width_px: resolution.width(),
                            height_px: resolution.height(),
                            encoding: CameraPreviewFrameEncoding::Jpeg,
                            frame_bytes: buffer.buffer().to_vec(),
                        }
                    } else {
                        let decoded_frame = buffer
                            .decode_image::<nokhwa::pixel_format::RgbFormat>()
                            .expect("camera frame should decode to RGB");
                        CameraPreviewFrame {
                            width_px: decoded_frame.width(),
                            height_px: decoded_frame.height(),
                            encoding: CameraPreviewFrameEncoding::Rgb24,
                            frame_bytes: decoded_frame.into_raw(),
                        }
                    };

                    let mut latest_frame_guard = latest_frame
                        .lock()
                        .expect("latest frame mutex should not be poisoned");
                    *latest_frame_guard = Some(frame.clone());

                    if !qr_enabled.load(std::sync::atomic::Ordering::Acquire) {
                        return;
                    }

                    let should_decode_now = {
                        let mut last_decode_guard = qr_last_decode_at
                            .lock()
                            .expect("qr decode timestamp mutex should not be poisoned");
                        let now = std::time::Instant::now();
                        let ready = match *last_decode_guard {
                            Some(last) => now.duration_since(last) >= std::time::Duration::from_millis(200),
                            None => true,
                        };
                        if ready {
                            *last_decode_guard = Some(now);
                        }
                        ready
                    };
                    if !should_decode_now {
                        return;
                    }

                    let listener_for_qr = {
                        let listener_guard = qr_listener
                            .lock()
                            .expect("qr listener mutex should not be poisoned");
                        listener_guard.as_ref().map(std::sync::Arc::clone)
                    };

                    if let Some(listener_for_qr) = listener_for_qr {
                        if let Err(error) = publish_qr_for_frame(
                            &listener_for_qr,
                            &frame.encoding,
                            frame.width_px,
                            frame.height_px,
                            &frame.frame_bytes,
                        ) {
                            listener_for_qr
                                .on_camera_qr_error(format!("failed preparing qr frame: {error}"));
                        }
                    }
                },
            )
            .map_err(|error| FfiError::from(eyre::eyre!("failed creating stream: {error}")))?;

            stream
                .open_stream()
                .map_err(|error| FfiError::from(eyre::eyre!("failed opening stream: {error}")))?;

            state_guard.stream = Some(stream);
            Ok(())
        }

        #[cfg(not(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        )))]
        {
            let _ = device_id;
            let _ = listener;
            panic_unsupported();
        }
    }

    pub fn stop_stream(&self) -> Result<(), FfiError> {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            let mut state_guard = self
                .state
                .lock()
                .expect("camera state mutex should not be poisoned");
            if let Some(mut stream) = state_guard.stream.take() {
                stream.stop_stream().map_err(|error| {
                    FfiError::from(eyre::eyre!("failed stopping stream: {error}"))
                })?;
            }

            let mut latest_frame_guard = state_guard
                .latest_frame
                .lock()
                .expect("latest frame mutex should not be poisoned");
            *latest_frame_guard = None;
            state_guard
                .qr_enabled
                .store(false, std::sync::atomic::Ordering::Release);
            Ok(())
        }

        #[cfg(not(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        )))]
        {
            panic_unsupported();
        }
    }

    pub fn latest_frame(&self) -> Option<CameraPreviewFrame> {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            let state_guard = self
                .state
                .lock()
                .expect("camera state mutex should not be poisoned");
            let latest_frame_guard = state_guard
                .latest_frame
                .lock()
                .expect("latest frame mutex should not be poisoned");
            latest_frame_guard.clone()
        }

        #[cfg(not(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        )))]
        {
            panic_unsupported();
        }
    }

    pub fn take_latest_frame(&self) -> Option<CameraPreviewFrame> {
        #[cfg(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        ))]
        {
            let state_guard = self
                .state
                .lock()
                .expect("camera state mutex should not be poisoned");
            let mut latest_frame_guard = state_guard
                .latest_frame
                .lock()
                .expect("latest frame mutex should not be poisoned");
            latest_frame_guard.take()
        }

        #[cfg(not(all(
            feature = "nokhwa",
            any(target_os = "linux", target_os = "macos", target_os = "windows")
        )))]
        {
            panic_unsupported();
        }
    }
}
