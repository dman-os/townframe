import { DEFAULT_COLLAPSED_CONTEXT_THRESHOLD } from '../constants';
import type {
  ChangeContent,
  FileDiffMetadata,
  Hunk,
  HunkExpansionRegion,
} from '../types';
import { getHunkSideStartBoundary } from './getHunkSideBoundaries';
import {
  getExpandedRegion,
  getTrailingExpandedRegion,
} from './virtualDiffLayout';

export interface DiffLineMetadata {
  unifiedLineIndex: number;
  splitLineIndex: number;
  lineIndex: number;
  lineNumber: number;
  noEOFCR: boolean;
}

export interface DiffLineCallbackBase {
  hunkIndex: number;
  hunk: Hunk | undefined; // undefined for trailing expansion region
  collapsedBefore: number; // > 0 means separator before this line, value = hidden lines
  collapsedAfter: number; // > 0 only on final line if trailing collapsed content
}

interface DiffLineCallbackContextChange extends DiffLineCallbackBase {
  type: 'change' | 'context' | 'context-expanded';
  deletionLine: DiffLineMetadata;
  additionLine: DiffLineMetadata;
}

interface DiffLineCallbackChangeDeletion extends DiffLineCallbackBase {
  type: 'change';
  deletionLine: DiffLineMetadata;
  additionLine?: undefined;
}

interface DiffLineCallbackChangeAddition extends DiffLineCallbackBase {
  type: 'change';
  deletionLine?: undefined;
  additionLine: DiffLineMetadata;
}

export type DiffLineCallbackProps =
  | DiffLineCallbackContextChange
  | DiffLineCallbackChangeDeletion
  | DiffLineCallbackChangeAddition;

type DiffStyle = 'unified' | 'split' | 'both';

type LineIterationBounds = [startIndex: number, endIndex: number];

type ChangeContentSide = 'deletions' | 'additions';

type ContextLineCallback = (index: number) => boolean | void;

interface IterationState {
  isWindowedHighlight: boolean;
  viewportStart: number;
  viewportEnd: number;
  splitCount: number;
  unifiedCount: number;
  finalHunkIndex: number;
  shouldBreak(): boolean;
  shouldSkip(unifiedHeight: number, splitHeight: number): boolean;
  incrementCounts(unifiedValue: number, splitValue: number): void;
  isInWindow(unifiedHeight: number, splitHeight: number): boolean;
  isInUnifiedWindow(height: number): boolean;
  isInSplitWindow(height: number): boolean;
  emit(props: DiffLineCallbackProps, silent?: boolean): boolean;
}

interface IterationStartState {
  hunkIndex: number;
  splitCount: number;
  unifiedCount: number;
}

interface HunkPrefixCounts {
  splitCount: number;
  unifiedCount: number;
}

interface IterationStartStateProps extends Omit<
  IterateOverDiffProps,
  'callback' | 'totalLines'
> {
  startingLine: number;
  collapsedContextThreshold: number;
}

interface HunkPrefixCountsProps extends Pick<
  IterationStartStateProps,
  'diff' | 'expandedHunks' | 'collapsedContextThreshold'
> {}

export type DiffLineCallback = (props: DiffLineCallbackProps) => boolean | void;

export interface IterateOverDiffProps {
  diff: FileDiffMetadata;
  diffStyle: DiffStyle;
  startingLine?: number;
  totalLines?: number;
  expandedHunks?: Map<number, HunkExpansionRegion> | true;
  collapsedContextThreshold?: number;
  callback: DiffLineCallback;
}

export function iterateOverDiff({
  diff,
  diffStyle,
  startingLine = 0,
  totalLines = Infinity,
  expandedHunks,
  collapsedContextThreshold = DEFAULT_COLLAPSED_CONTEXT_THRESHOLD,
  callback,
}: IterateOverDiffProps): void {
  const iterationStart = getIterationStartState({
    diff,
    diffStyle,
    startingLine,
    expandedHunks,
    collapsedContextThreshold,
  });
  const state: IterationState = {
    viewportStart: startingLine,
    viewportEnd: startingLine + totalLines,
    isWindowedHighlight: startingLine > 0 || totalLines < Infinity,
    splitCount: iterationStart.splitCount,
    unifiedCount: iterationStart.unifiedCount,
    finalHunkIndex: diff.hunks.length - 1,
    shouldBreak() {
      if (!state.isWindowedHighlight) {
        return false;
      }

      const breakUnified = state.unifiedCount >= startingLine + totalLines;
      const breakSplit = state.splitCount >= startingLine + totalLines;

      if (diffStyle === 'unified') {
        return breakUnified;
      } else if (diffStyle === 'split') {
        return breakSplit;
      } else {
        return breakUnified && breakSplit;
      }
    },
    shouldSkip(unifiedCount: number, splitCount: number) {
      if (!state.isWindowedHighlight) {
        return false;
      }

      const skipUnified =
        unifiedCount > 0 && state.unifiedCount + unifiedCount <= startingLine;
      const skipSplit =
        splitCount > 0 && state.splitCount + splitCount <= startingLine;

      if (diffStyle === 'unified') {
        return skipUnified;
      } else if (diffStyle === 'split') {
        return skipSplit;
      } else {
        return skipUnified && skipSplit;
      }
    },
    incrementCounts(unifiedValue: number, splitValue: number) {
      if (diffStyle === 'unified' || diffStyle === 'both') {
        state.unifiedCount += unifiedValue;
      }
      if (diffStyle === 'split' || diffStyle === 'both') {
        state.splitCount += splitValue;
      }
    },
    isInWindow(unifiedHeight: number, splitHeight: number) {
      if (!state.isWindowedHighlight) {
        return true;
      }

      const unifiedInWindow = state.isInUnifiedWindow(unifiedHeight);
      const splitInWindow = state.isInSplitWindow(splitHeight);

      if (diffStyle === 'unified') {
        return unifiedInWindow;
      } else if (diffStyle === 'split') {
        return splitInWindow;
      } else {
        return unifiedInWindow || splitInWindow;
