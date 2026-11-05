import { FileDiff } from '@pierre/diffs';
import oldContents from './old.ts?raw';
import newContents from './new.ts?raw';
const mode = new URLSearchParams(location.search);
const options = {
  theme: { dark: 'pierre-dark', light: 'pierre-light' },
  themeType: mode.get('theme') ?? 'dark',
  diffStyle: 'unified',
  expandUnchanged: true,
  disableFileHeader: true,
  overflow: 'scroll',
  useCSSClasses: mode.get('classes') === '1',
};
const inst = new FileDiff(options);
inst.render({
  oldFile: { name: 'iterateOverDiff.ts', contents: oldContents },
  newFile: { name: 'iterateOverDiff.ts', contents: newContents },
  containerWrapper: document.getElementById('root'),
});
window.__pierre = inst;
