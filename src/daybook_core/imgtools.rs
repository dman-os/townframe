use crate::interlude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownsizeImageJpegResult {
    pub bytes: Vec<u8>,
    pub mime: String,
    pub width: u32,
    pub height: u32,
}

pub fn downsize_image_jpeg(
    bytes: &[u8],
    max_side: u32,
    jpeg_quality: u8,
) -> Res<DownsizeImageJpegResult> {
    if bytes.is_empty() {
        eyre::bail!("empty image bytes");
    }
    if max_side == 0 {
        eyre::bail!("max_side must be > 0");
    }

    let image = image::load_from_memory(bytes).wrap_err("error decoding image bytes")?;
    let src_width = image.width();
    let src_height = image.height();
    if src_width == 0 || src_height == 0 {
        eyre::bail!("decoded image has zero dimension");
    }

    let resized = if src_width.max(src_height) > max_side {
        image.resize(max_side, max_side, image::imageops::FilterType::Lanczos3)
    } else {
        image
    };

    let width = resized.width();
    let height = resized.height();
    let rgb8 = resized.to_rgb8();
    let mut out = Vec::new();
    {
        let mut encoder =
            image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, jpeg_quality);
        encoder
            .encode(rgb8.as_raw(), width, height, image::ColorType::Rgb8.into())
            .wrap_err("error encoding jpeg")?;
    }

    Ok(DownsizeImageJpegResult {
        bytes: out,
        mime: "image/jpeg".to_string(),
        width,
        height,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_png(width: u32, height: u32) -> Vec<u8> {
        let mut img = image::RgbImage::new(width, height);
        for (x, y, pixel) in img.enumerate_pixels_mut() {
            *pixel = image::Rgb([(x % 255) as u8, (y % 255) as u8, 128]);
        }
        let dyn_img = image::DynamicImage::ImageRgb8(img);
        let mut out = Vec::new();
        dyn_img
            .write_to(&mut std::io::Cursor::new(&mut out), image::ImageFormat::Png)
            .expect("png encode");
        out
    }

    #[test]
    fn downsizes_large_image_preserving_aspect_ratio() -> Res<()> {
        let bytes = make_png(1600, 900);
        let result = downsize_image_jpeg(&bytes, 800, 80)?;
        assert_eq!(result.mime, "image/jpeg");
        assert_eq!(result.width, 800);
        assert_eq!(result.height, 450);
        assert!(!result.bytes.is_empty());
        Ok(())
    }

    #[test]
    fn does_not_upscale_small_image() -> Res<()> {
        let bytes = make_png(200, 120);
        let result = downsize_image_jpeg(&bytes, 800, 80)?;
        assert_eq!(result.width, 200);
        assert_eq!(result.height, 120);
        Ok(())
    }

    #[test]
    fn jpeg_output_decodes() -> Res<()> {
        let bytes = make_png(320, 200);
        let result = downsize_image_jpeg(&bytes, 256, 75)?;
        let decoded = image::load_from_memory(&result.bytes)?;
        assert_eq!(decoded.width(), result.width);
        assert_eq!(decoded.height(), result.height);
        Ok(())
    }
}
