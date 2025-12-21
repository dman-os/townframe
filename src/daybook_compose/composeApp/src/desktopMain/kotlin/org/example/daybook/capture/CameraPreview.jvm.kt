package org.example.daybook.capture

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.awt.SwingPanel
import androidx.compose.ui.unit.dp
import com.github.sarxos.webcam.Webcam
import com.github.sarxos.webcam.WebcamPanel
import com.github.sarxos.webcam.WebcamResolution
import com.github.sarxos.webcam.ds.buildin.WebcamDefaultDriver
import com.github.sarxos.webcam.ds.v4l4j.V4l4jDriver
import com.github.sarxos.webcam.ds.gstreamer.GStreamerDriver
import com.github.sarxos.webcam.ds.openimaj.OpenImajDriver
import com.github.sarxos.webcam.ds.javacv.JavaCvDriver
import com.github.sarxos.webcam.ds.vlcj.VlcjDriver
import com.github.sarxos.webcam.ds.fswebcam.FsWebcamDriver
// LTI-CIVIL driver package name may vary - uncomment when package is confirmed
// import com.github.sarxos.webcam.ds.lti.LtiCivilDriver
import com.github.sarxos.webcam.ds.jmf.JmfDriver
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.example.daybook.capture.LocalCameraCaptureContext
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO

/**
 * Available webcam drivers for selection.
 * All drivers from webcam-capture library are included.
 */
enum class WebcamDriverType(
    val displayName: String,
    val description: String,
    val createDriver: () -> com.github.sarxos.webcam.WebcamDriver
) {
    DEFAULT(
        displayName = "Default (OpenIMAJ)",
        description = "Built-in driver for Windows, Linux, macOS. Requires libv4l2 and libstdc++ on Linux.",
        createDriver = { WebcamDefaultDriver() }
    ),
    V4L4J(
        displayName = "V4L4j",
        description = "Linux-only driver using V4L4j library. Good for Raspberry Pi. Requires v4l-utils.",
        createDriver = { V4l4jDriver() }
    ),
    GSTREAMER(
        displayName = "GStreamer",
        description = "Windows and Linux. Uses GStreamer framework. Requires GStreamer installed.",
        createDriver = { GStreamerDriver() }
    ),
    OPENIMAJ(
        displayName = "OpenIMAJ",
        description = "Uses OpenIMAJ framework to access UVC cameras. Cross-platform.",
        createDriver = { OpenImajDriver() }
    ),
    JAVACV(
        displayName = "JavaCV (OpenCV)",
        description = "Uses JavaCV bindings for OpenCV. Requires OpenCV native libraries.",
        createDriver = { JavaCvDriver() }
    ),
    VLCJ(
        displayName = "VLCj",
        description = "Uses VLCj library to access UVC cameras. Requires VLC media player.",
        createDriver = { VlcjDriver() }
    ),
    FSWEBCAM(
        displayName = "FsWebcam",
        description = "Uses fswebcam command-line tool. Unix-like systems only. Requires fswebcam installed.",
        createDriver = { FsWebcamDriver() }
    ),
    // LTI_CIVIL driver - package name needs to be confirmed
    // LTI_CIVIL(
    //     displayName = "LTI-CIVIL",
    //     description = "Uses LTI-CIVIL library. Supports wide range of UVC devices. 32-bit only.",
    //     createDriver = { LtiCivilDriver() }
    // ),
    JMF(
        displayName = "JMF (Java Media Framework)",
        description = "Uses Java Media Framework. Requires JMF installed and configured.",
        createDriver = { JmfDriver() }
    ),
}

@Composable
actual fun DaybookCameraPreview(
    modifier: Modifier,
    onImageSaved: ((ByteArray) -> Unit)?,
    onCaptureRequested: (() -> Unit)?,
) {
    val captureContext = LocalCameraCaptureContext.current
    val scope = rememberCoroutineScope()
    
    var selectedDriver by remember { mutableStateOf(WebcamDriverType.DEFAULT) }
    var showDriverMenu by remember { mutableStateOf(false) }
    
    // Set driver and get webcams
    val webcams = remember(selectedDriver) {
        runCatching {
            // Set the selected driver
            val driver = selectedDriver.createDriver()
            Webcam.setDriver(driver)
            Webcam.getWebcams()
        }.getOrNull()
    }
    
    val webcam = remember(webcams) {
        webcams?.firstOrNull()?.also { w ->
            // Close it first to ensure clean state
            if (w.isOpen) {
                w.close()
            }
            // Set view size to a reasonable resolution
            w.viewSize = WebcamResolution.VGA.size
        }
    }
    
    // Set up capture callback
    LaunchedEffect(captureContext, webcam) {
        if (captureContext != null && webcam != null) {
            val w = webcam!!
            captureContext.setCanCapture(true)
            captureContext.setCaptureCallback {
                scope.launch {
                    try {
                        captureContext.setIsCapturing(true)
                        
                        // Capture image on background thread
                        val image = withContext(Dispatchers.IO) {
                            w.image
                        }
                        
                        if (image != null) {
                            // Convert BufferedImage to JPEG ByteArray
                            val baos = ByteArrayOutputStream()
                            ImageIO.write(image, "jpg", baos)
                            val bytes = baos.toByteArray()
                            
                            onImageSaved?.invoke(bytes)
                        }
                        
                        captureContext.setIsCapturing(false)
                    } catch (e: Exception) {
                        println("Error capturing image: ${e.message}")
                        captureContext.setIsCapturing(false)
                    }
                }
            }
        } else {
            captureContext?.setCanCapture(false)
            captureContext?.setCaptureCallback(null)
        }
    }
    
    // Create WebcamPanel for preview
    val panel = remember(webcam) {
        webcam?.let { w ->
            WebcamPanel(w, false).apply {
                isFPSDisplayed = false
                isDisplayDebugInfo = false
                isImageSizeDisplayed = false
                isMirrored = false
            }
        }
    }
    
    // Cleanup webcam and panel on dispose
    DisposableEffect(webcam, panel) {
        onDispose {
            captureContext?.setCaptureCallback(null)
            captureContext?.setCanCapture(false)
            
            panel?.let { p ->
                try {
                    if (p.isStarted) {
                        p.stop()
                    }
                } catch (e: Exception) {
                    println("Error stopping panel: ${e.message}")
                }
            }
            
            webcam?.let { w ->
                try {
                    if (w.isOpen) {
                        w.close()
                    }
                } catch (e: Exception) {
                    println("Error closing webcam: ${e.message}")
                }
            }
        }
    }
    
    Box(modifier = modifier.fillMaxSize()) {
        when {
            webcams == null || webcam == null -> {
                Column(
                    modifier = Modifier.fillMaxSize(),
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    Box(
                        modifier = Modifier
                            .fillMaxSize()
                            .weight(1f),
                        contentAlignment = Alignment.Center
                    ) {
                        val error = runCatching {
                            val driver = selectedDriver.createDriver()
                            Webcam.setDriver(driver)
                            Webcam.getWebcams()
                        }.exceptionOrNull()
                        
                        Text(
                            error?.message ?: "No webcam found. Try a different driver."
                        )
                    }
                    
                    // Driver selection menu
                    Box(
                        modifier = Modifier.padding(16.dp),
                        contentAlignment = Alignment.BottomCenter
                    ) {
                        DriverSelectionMenu(
                            selectedDriver = selectedDriver,
                            onDriverSelected = { selectedDriver = it },
                            expanded = showDriverMenu,
                            onExpandedChange = { showDriverMenu = it }
                        )
                    }
                }
            }
            else -> {
                Column(modifier = Modifier.fillMaxSize()) {
                    // Driver selection menu at top
                    Box(
                        modifier = Modifier.padding(8.dp),
                        contentAlignment = Alignment.TopEnd
                    ) {
                        DriverSelectionMenu(
                            selectedDriver = selectedDriver,
                            onDriverSelected = { selectedDriver = it },
                            expanded = showDriverMenu,
                            onExpandedChange = { showDriverMenu = it }
                        )
                    }
                    
                    // Camera preview using WebcamPanel
                    if (panel != null) {
                        SwingPanel(
                            factory = { 
                                panel.apply {
                                    if (!webcam.isOpen) {
                                        webcam.open()
                                    }
                                    if (!isStarted) {
                                        start()
                                    }
                                }
                            },
                            modifier = Modifier.fillMaxSize()
                        )
                    }
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun DriverSelectionMenu(
    selectedDriver: WebcamDriverType,
    onDriverSelected: (WebcamDriverType) -> Unit,
    expanded: Boolean,
    onExpandedChange: (Boolean) -> Unit
) {
    ExposedDropdownMenuBox(
        expanded = expanded,
        onExpandedChange = onExpandedChange
    ) {
        TextField(
            value = selectedDriver.displayName,
            onValueChange = {},
            readOnly = true,
            trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = expanded) },
            modifier = Modifier.menuAnchor()
        )
        ExposedDropdownMenu(
            expanded = expanded,
            onDismissRequest = { onExpandedChange(false) }
        ) {
            WebcamDriverType.entries.forEach { driver ->
                DropdownMenuItem(
                    text = { 
                        Column {
                            Text(driver.displayName)
                            Text(
                                text = driver.description,
                                style = androidx.compose.material3.MaterialTheme.typography.bodySmall
                            )
                        }
                    },
                    onClick = {
                        onDriverSelected(driver)
                        onExpandedChange(false)
                    }
                )
            }
        }
    }
}
