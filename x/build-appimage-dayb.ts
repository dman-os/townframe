#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

/**
 * Ubuntu requirements (installed automatically by this script when possible):
 * - build/runtime tools: binutils coreutils desktop-file-utils file patchelf
 * - packaging tools: libarchive-tools xz-utils squashfs-tools strace util-linux
 * - appimage/runtime helpers: libgdk-pixbuf2.0-dev libglib2.0-bin zsync
 * - fetching/install helpers: ca-certificates wget
 *
 * To skip apt installation, set DAYBOOK_SKIP_APT_INSTALL=1.
 */

const composeRoot = $.relativeDir("../src/daybook_compose/");
const composeAppDir = composeRoot.join(
  "composeApp",
  "build",
  "compose",
  "binaries",
  "main",
  "app",
  "org.example.daybook",
);

const appImageToolsDir = $.relativeDir("../target/appimage-tools/");
const outputDir = $.relativeDir("../target/appimage/");
const stageAppDir = $.path("/tmp/DaybookJvmLinuxDeploy.AppDir");
const desktopId = "org.example.daybook";
const appName = "daybook";

const machine = (await $`uname -m`.text()).trim();
const arch = machine === "aarch64" || machine === "arm64"
  ? "aarch64"
  : "x86_64";
if (!(arch === "x86_64" || arch === "aarch64")) {
  throw new Error(`unsupported architecture: ${machine}`);
}

const outputSuffix = arch === "aarch64" ? "arm64" : "x64";
const outputPath = outputDir.join(
  `daybook-linuxdeploy-${outputSuffix}.AppImage`,
);
const tarballPath = outputDir.join(
  `daybook-linuxdeploy-${outputSuffix}.tar.xz`,
);
const linuxdeployToolPath = appImageToolsDir.join(
  `linuxdeploy-${arch}.AppImage`,
);
const linuxdeployUrl = $.env.DAYBOOK_LINUXDEPLOY_URL ??
  `https://github.com/linuxdeploy/linuxdeploy/releases/download/1-alpha-20251107-1/linuxdeploy-${arch}.AppImage`;

const stageUsrDir = stageAppDir.join("usr");
const stageBinDir = stageUsrDir.join("bin");
const stageLibexecDir = stageUsrDir.join("libexec", appName);
const stageAppLibDir = stageLibexecDir.join("lib", "app");
const stageDesktopPath = stageAppDir.join(`${desktopId}.desktop`);
const stageIconPath = stageAppDir.join(`${desktopId}.png`);
const stageLauncherPath = stageBinDir.join(appName);
const stagedMainExecutablePath = stageLibexecDir.join("bin", desktopId);
const stagedCfgPath = stageLibexecDir.join("lib", "app", `${desktopId}.cfg`);
const repoRoot = $.relativeDir("../");

async function ensureUbuntuDeps() {
  if (Deno.env.get("DAYBOOK_SKIP_APT_INSTALL") === "1") {
    console.log(
      "Skipping apt dependency installation (DAYBOOK_SKIP_APT_INSTALL=1)",
    );
    return;
  }

  if (Deno.build.os !== "linux") {
    console.log("Skipping apt dependency installation (non-linux host)");
    return;
  }
  if ((await $`command -v apt-get`.noThrow()).code !== 0) {
    console.log("Skipping apt dependency installation (apt-get not found)");
    return;
  }

  const packages = [
    "binutils",
    "ca-certificates",
    "coreutils",
    "desktop-file-utils",
    "file",
    "libarchive-tools",
    "libgdk-pixbuf2.0-dev",
    "libglib2.0-bin",
    "patchelf",
    "protobuf-compiler",
    "squashfs-tools",
    "strace",
    "util-linux",
    "wget",
    "xz-utils",
    "zsync",
  ];

  const isRoot = (await $`id -u`.text()).trim() === "0";
  const hasSudo = (await $`command -v sudo`.noThrow()).code === 0;
  if (isRoot) {
    await $`apt-get update`;
    await $`apt-get install -y ${packages}`;
    return;
  }
  if (!hasSudo) {
    throw new Error("apt-get requires root privileges and sudo is unavailable");
  }
  await $`sudo apt-get update`;
  await $`sudo apt-get install -y ${packages}`;
}

async function findFfiSoPath() {
  const candidatePaths = [
    repoRoot.join("target", "debug", "libdaybook_ffi.so"),
    repoRoot.join("target", "release", "libdaybook_ffi.so"),
  ];
  for (const candidatePath of candidatePaths) {
    if (await candidatePath.exists()) {
      return candidatePath;
    }
  }
  throw new Error(
    `missing libdaybook_ffi.so (checked: ${
      candidatePaths
        .map((path) => path.toString())
        .join(", ")
    })`,
  );
}

async function ensureFfiSoPath() {
  const existingPath = await findFfiSoPath().catch(() => null);
  if (existingPath) {
    return existingPath;
  }
  await $`cargo build -p daybook_ffi --features nokhwa`;
  return await findFfiSoPath();
}

await ensureUbuntuDeps();
await $`./gradlew :composeApp:packageAppImage --no-daemon --no-configuration-cache`
  .cwd(composeRoot)
  .env({
    LD_LIBRARY_PATH: "",
    DYLD_LIBRARY_PATH: "",
  });

if (!(await composeAppDir.exists())) {
  throw new Error(`missing compose app dir: ${composeAppDir}`);
}

await appImageToolsDir.ensureDir();
if (!(await linuxdeployToolPath.exists())) {
  await $.request(linuxdeployUrl)
    .showProgress()
    .pipeToPath(linuxdeployToolPath);
}
await linuxdeployToolPath.chmod(0o755);

if (await stageAppDir.exists()) {
  await stageAppDir.remove({ recursive: true });
}
await stageBinDir.ensureDir();
await composeAppDir.copy(stageLibexecDir);
await stageAppLibDir.ensureDir();

const ffiSoPath = await ensureFfiSoPath();
await ffiSoPath.copyFile(stageAppLibDir.join("libdaybook_ffi.so"));

if (!(await stagedMainExecutablePath.exists())) {
  throw new Error(`missing staged executable: ${stagedMainExecutablePath}`);
}

if (!(await stagedCfgPath.exists())) {
  throw new Error(`missing cfg file: ${stagedCfgPath}`);
}
const cfgText = await stagedCfgPath.readText();
const cfgLinesToAdd = [
  "java-options=-Djava.library.path=$APPDIR/lib/app",
  "java-options=-Djna.boot.library.path=$APPDIR/lib/app",
  "java-options=-Djna.nosys=true",
];
const missingCfgLines = cfgLinesToAdd.filter((line) => !cfgText.includes(line));
if (missingCfgLines.length > 0) {
  await stagedCfgPath.writeText(
    `${cfgText.trimEnd()}\n${missingCfgLines.join("\n")}\n`,
  );
}

await stageLauncherPath.writeText(
  [
    "#!/usr/bin/env bash",
    "set -euo pipefail",
    'script_path="$(readlink -f "$0")"',
    'script_dir="$(cd "$(dirname "$script_path")" && pwd)"',
    `exec "$script_dir/../libexec/${appName}/bin/${desktopId}" "$@"`,
  ].join("\n") + "\n",
);
await stageLauncherPath.chmod(0o755);

await stageDesktopPath.writeText(
  [
    "[Desktop Entry]",
    "Type=Application",
    "Name=Daybook",
    "Comment=Daybook Desktop Application",
    `Exec=${appName}`,
    `Icon=${desktopId}`,
    "Categories=Office;",
  ].join("\n") + "\n",
);

const iconSourcePath = composeRoot.join(
  "composeApp",
  "src",
  "androidMain",
  "res",
  "mipmap-xxxhdpi",
  "ic_launcher.png",
);
if (!(await iconSourcePath.exists())) {
  throw new Error(`missing icon source: ${iconSourcePath}`);
}
await iconSourcePath.copyFile(stageIconPath);

await $`find ${stageAppDir} -type f -print0 | xargs -0 chmod u+w`;

await outputDir.ensureDir();
if (await outputPath.exists()) {
  await outputPath.remove();
}
if (await tarballPath.exists()) {
  await tarballPath.remove();
}

await $`bsdtar -c -J -f ${tarballPath} --format=gnutar --options xz:compression-level=9,xz:threads=0 -C ${stageUsrDir} .`;

const outputValue = outputPath.toString();
await $`${linuxdeployToolPath}
  --appdir ${stageAppDir}
  --desktop-file ${stageDesktopPath}
  --icon-file ${stageIconPath}
  --icon-filename ${desktopId}
  --deploy-deps-only ${stagedMainExecutablePath}
  --deploy-deps-only ${stageLibexecDir.join("lib")}
  --deploy-deps-only ${stageLibexecDir.join("lib", "app")}
  --deploy-deps-only ${stageLibexecDir.join("lib", "runtime", "lib")}
  --output appimage`.env({
  APPIMAGE_EXTRACT_AND_RUN: "1",
  LDAI_OUTPUT: outputValue,
  OUTPUT: outputValue,
  LD_LIBRARY_PATH: "",
  DYLD_LIBRARY_PATH: "",
});

console.log(`Created tarball: ${tarballPath}`);
console.log(`Created AppImage: ${outputPath}`);
