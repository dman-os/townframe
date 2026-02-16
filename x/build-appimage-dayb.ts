#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const repoRoot = $.relativeDir("../");
const composeRoot = $.relativeDir("../src/daybook_compose/");
const appDir = composeRoot.join(
	"composeApp",
	"build",
	"compose",
	"binaries",
	"main-release",
	"app",
	"org.example.daybook",
);
const appLibDir = appDir.join("lib", "app");
const appConfigPath = appLibDir.join("org.example.daybook.cfg");

const nativeLibraryPaths = [
	$.relativeDir("../target/release/libdaybook_ffi.so"),
];

const appImageToolsDir = $.relativeDir("../target/appimage-tools/");
const appImageToolPath = appImageToolsDir.join("appimagetool-x86_64.AppImage");
const appImageToolUrl =
	$.env.DAYBOOK_APPIMAGETOOL_URL ??
	"https://github.com/AppImage/appimagetool/releases/download/continuous/appimagetool-x86_64.AppImage";

const outputDir = $.relativeDir("../target/appimage/");
const outputPath = outputDir.join("daybook-linux-x86_64.AppImage");

await $`cargo build -p daybook_ffi --release`.cwd(repoRoot);
await $`./gradlew :composeApp:packageReleaseAppImage --no-daemon`.cwd(
	composeRoot,
);

await appImageToolsDir.ensureDir();
if (!(await appImageToolPath.exists())) {
	await $.request(appImageToolUrl).showProgress().pipeToPath(appImageToolPath);
	await appImageToolPath.chmod(0o755);
}

for (const nativeLibraryPath of nativeLibraryPaths) {
	if (!(await nativeLibraryPath.exists())) {
		throw new Error(`missing native library: ${nativeLibraryPath}`);
	}
	await nativeLibraryPath.copyFileToDir(appLibDir);
}

const configText = await appConfigPath.readText();
const configLinesToAdd = [
	"java-options=-Djava.library.path=$APPDIR",
	"java-options=-Djna.library.path=$APPDIR",
];
const missingConfigLines = configLinesToAdd.filter(
	(line) => !configText.includes(line),
);
if (missingConfigLines.length > 0) {
	const patchedConfigText = `${configText.trimEnd()}\n${missingConfigLines.join("\n")}\n`;
	await appConfigPath.writeText(patchedConfigText);
}

const iconSourcePath = appDir.join("lib", "org.example.daybook.png");
const iconDestPath = appDir.join("org.example.daybook.png");
if (!(await iconSourcePath.exists())) {
	throw new Error(`missing icon source: ${iconSourcePath}`);
}
await iconSourcePath.copyFile(iconDestPath);

const desktopFilePath = appDir.join("org.example.daybook.desktop");
await desktopFilePath.writeText(
	[
		"[Desktop Entry]",
		"Type=Application",
		"Name=Daybook",
		"Comment=Daybook Desktop Application",
		"Exec=org.example.daybook",
		"Icon=org.example.daybook",
		"Categories=Office;",
	].join("\n") + "\n",
);

const appRunPath = appDir.join("AppRun");
await appRunPath.writeText(
	[
		"#!/bin/sh",
		"set -eu",
		'HERE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"',
		'exec "$HERE/bin/org.example.daybook" "$@"',
	].join("\n") + "\n",
);
await appRunPath.chmod(0o755);

await outputDir.ensureDir();
if (await outputPath.exists()) {
	await outputPath.remove();
}

await $`appimage-run ${appImageToolPath} ${appDir} ${outputPath}`.env({
	ARCH: "x86_64",
});

console.log(`AppImage created at: ${outputPath}`);
