// Lifted from https://github.com/mrousavy/react-native-vision-camera/blob/ea097d20fd53b70b42fd04e6dc2442d5ddd2bf67/example/src/hooks/usePreferredCameraDevice.ts
// MIT Marc Rousavy

// import { useMMKVString } from 'react-native-mmkv'
import { useCallback, useMemo, useState } from 'react'
import type { CameraDevice } from 'react-native-vision-camera'
import { useCameraDevices } from 'react-native-vision-camera'

export function usePreferredCameraDevice(): [CameraDevice | undefined, (device: CameraDevice) => void] {
  const [preferredDeviceId, setPreferredDeviceId] = useState<string>()
  // const [preferredDeviceId, setPreferredDeviceId] = useMMKVString('camera.preferredDeviceId')

  const set = useCallback(
    (device: CameraDevice) => {
      setPreferredDeviceId(device.id)
    },
    [setPreferredDeviceId],
  )

  const devices = useCameraDevices()
  const device = useMemo(() => devices.find((d) => d.id === preferredDeviceId), [devices, preferredDeviceId])

  return [device, set]
}
