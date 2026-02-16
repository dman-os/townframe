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
#[derive(Default)]
struct DesktopCameraState {
    stream: Option<nokhwa::CallbackCamera>,
    latest_frame: std::sync::Arc<std::sync::Mutex<Option<CameraPreviewFrame>>>,
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
                    *latest_frame_guard = Some(frame);
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
