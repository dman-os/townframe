// Lifted from https://github.com/mrousavy/react-native-vision-camera/blob/ea097d20fd53b70b42fd04e6dc2442d5ddd2bf67/example/src/hooks/useIsForeground.ts
// MIT Marc Rousavy

import { useState } from 'react'
import { useEffect } from 'react'
import type { AppStateStatus } from 'react-native'
import { AppState } from 'react-native'

export const useIsForeground = (): boolean => {
  const [isForeground, setIsForeground] = useState(true)

  useEffect(() => {
    const onChange = (state: AppStateStatus): void => {
      setIsForeground(state === 'active')
    }
    const listener = AppState.addEventListener('change', onChange)
    return () => listener.remove()
  }, [setIsForeground])

  return isForeground
}
