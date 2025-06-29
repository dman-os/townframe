// Lifted from https://github.com/mrousavy/react-native-vision-camera/blob/ea097d20fd53b70b42fd04e6dc2442d5ddd2bf67/example/
// MIT Marc Rousavy

import { Dimensions, Platform } from 'react-native'
// import StaticSafeAreaInsets from 'react-native-static-safe-area-insets'
const StaticSafeAreaInsets = {
  safeAreaInsetsLeft: 0,
  safeAreaInsetsTop: 0,
  safeAreaInsetsRight: 0,
  safeAreaInsetsBottom: 0,
}

export const CONTENT_SPACING = 15

const SAFE_BOTTOM =
  Platform.select({
    ios: StaticSafeAreaInsets.safeAreaInsetsBottom,
  }) ?? 0

export const SAFE_AREA_PADDING = {
  paddingLeft: StaticSafeAreaInsets.safeAreaInsetsLeft + CONTENT_SPACING,
  paddingTop: StaticSafeAreaInsets.safeAreaInsetsTop + CONTENT_SPACING,
  paddingRight: StaticSafeAreaInsets.safeAreaInsetsRight + CONTENT_SPACING,
  paddingBottom: SAFE_BOTTOM + CONTENT_SPACING,
}

// The maximum zoom _factor_ you should be able to zoom in
export const MAX_ZOOM_FACTOR = 10

export const SCREEN_WIDTH = Dimensions.get('window').width
export const SCREEN_HEIGHT = Platform.select<number>({
  android: Dimensions.get('screen').height - StaticSafeAreaInsets.safeAreaInsetsBottom,
  ios: Dimensions.get('window').height,
}) as number

// Capture Button
export const CAPTURE_BUTTON_SIZE = 78

// Control Button like Flash
export const CONTROL_BUTTON_SIZE = 40
