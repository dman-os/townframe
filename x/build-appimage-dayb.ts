#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

const composeRoot = $.relativeDir("../src/daybook_compose/");
const composeAppDir = composeRoot.join(
  "composeApp",
  "build",
  "compose",
  "binaries",
  "main-release",
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
  `daybook-linuxdeploy-${outputSuffix}.tar.gz`,
);
const linuxdeployToolPath = appImageToolsDir.join(
  `linuxdeploy-${arch}.AppImage`,
);
const linuxdeployUrl = $.env.DAYBOOK_LINUXDEPLOY_URL ??
  `https://github.com/linuxdeploy/linuxdeploy/releases/download/1-alpha-20251107-1/linuxdeploy-${arch}.AppImage`;

const stageUsrDir = stageAppDir.join("usr");
const stageBinDir = stageUsrDir.join("bin");
const stageLibexecDir = stageUsrDir.join("libexec", appName);
const stageDesktopPath = stageAppDir.join(`${desktopId}.desktop`);
const stageIconPath = stageAppDir.join(`${desktopId}.png`);
const stageLauncherPath = stageBinDir.join(appName);
const stagedMainExecutablePath = stageLibexecDir.join("bin", desktopId);
const stagedCfgPath = stageLibexecDir.join("lib", "app", `${desktopId}.cfg`);

function ldLinuxPath() {
  if (arch === "aarch64") {
    return "/lib/ld-linux-aarch64.so.1";
  }
  return "/lib64/ld-linux-x86-64.so.2";
}

function pathContainsNixStore(pathText: string) {
  return pathText.includes("/nix/store/");
}

async function collectFiles(dirPath: string): Promise<string[]> {
  const out: string[] = [];
  for await (const entry of Deno.readDir(dirPath)) {
    const entryPath = `${dirPath}/${entry.name}`;
    if (entry.isDirectory) {
      out.push(...(await collectFiles(entryPath)));
    } else if (entry.isFile) {
      out.push(entryPath);
    }
  }
  return out;
}

async function isElf(filePath: string): Promise<boolean> {
  const file = await Deno.open(filePath, { read: true });
  try {
    const header = new Uint8Array(4);
    const read = await file.read(header);
    if (read !== 4) {
      return false;
    }
    return (
      header[0] === 0x7f &&
      header[1] === 0x45 &&
      header[2] === 0x4c &&
      header[3] === 0x46
    );
  } finally {
    file.close();
  }
}

async function patchInterpreters(appDirPath: ReturnType<typeof $.path>) {
  const files = await collectFiles(appDirPath.toString());
  for (const filePath of files) {
    if (!(await isElf(filePath))) {
      continue;
    }
    const programHeaders = await $`readelf -l ${filePath}`.noThrow().text();
    if (!programHeaders.includes("Requesting program interpreter:")) {
      continue;
    }
    const currentInterpreter = (
      await $`patchelf --print-interpreter ${filePath}`.text()
    ).trim();
    if (!currentInterpreter.startsWith("/")) {
      continue;
    }
    if (currentInterpreter !== ldLinuxPath()) {
      await $`patchelf --set-interpreter ${ldLinuxPath()} ${filePath}`;
    }
  }
}

const excludedCopiedLibs = new Set([
  "ld-linux-x86-64.so.2",
  "ld-linux-aarch64.so.1",
  "libc.so.6",
  "libdl.so.2",
  "libm.so.6",
  "libpthread.so.0",
  "librt.so.1",
  "libgcc_s.so.1",
  "libstdc++.so.6",
]);

async function copyResolvedSharedObjects(
  appDirPath: ReturnType<typeof $.path>,
  destLibDir: ReturnType<typeof $.path>,
) {
  await destLibDir.ensureDir();
  const files = await collectFiles(appDirPath.toString());
  const soPaths = new Set<string>();
  const missingSonames = new Set<string>();

  for (const filePath of files) {
    if (!(await isElf(filePath))) {
      continue;
    }
    const lddOutput = await $`ldd ${filePath}`.noThrow().text();
    for (const line of lddOutput.split("\n")) {
      const arrowIdx = line.indexOf("=>");
      if (arrowIdx < 0) {
        continue;
      }
      const rhs = line.slice(arrowIdx + 2).trim();
      if (!rhs.startsWith("/")) {
        continue;
      }
      const soPath = rhs.split(/\s+/)[0];
      if (soPath === "not") {
        const soName = line.slice(0, arrowIdx).trim().split(/\s+/)[0];
        if (soName && !excludedCopiedLibs.has(soName)) {
          missingSonames.add(soName);
        }
        continue;
      }
      if (!soPath.startsWith("/nix/store/")) {
        continue;
      }
      const soBaseName = soPath.split("/").pop();
      if (!soBaseName || excludedCopiedLibs.has(soBaseName)) {
        continue;
      }
      soPaths.add(soPath);
    }
  }

  for (const soName of missingSonames) {
    for await (const storeEntry of Deno.readDir("/nix/store")) {
      const candidatePath = `/nix/store/${storeEntry.name}/lib/${soName}`;
      const stat = await Deno.stat(candidatePath).catch(() => null);
      if (stat?.isFile) {
        soPaths.add(candidatePath);
        break;
      }
    }
  }

  for (const soPath of soPaths) {
    const soBaseName = soPath.split("/").pop();
    if (!soBaseName) {
      continue;
    }
    const realSoPath = await Deno.realPath(soPath).catch(() => soPath);
    const realSoBaseName = realSoPath.split("/").pop();
    if (!realSoBaseName) {
      continue;
    }
    const realDestPath = destLibDir.join(realSoBaseName);
    if (!(await realDestPath.exists())) {
      await $.path(realSoPath).copy(realDestPath);
    }

    if (realSoBaseName !== soBaseName) {
      const sonameDestPath = destLibDir.join(soBaseName);
      if (await sonameDestPath.exists()) {
        await sonameDestPath.remove();
      }
      await Deno.symlink(realSoBaseName, sonameDestPath.toString());
    }
  }
}

await $`./gradlew :composeApp:packageReleaseAppImage --no-daemon`
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
  await linuxdeployToolPath.chmod(0o755);
}

if (await stageAppDir.exists()) {
  await stageAppDir.remove({ recursive: true });
}
await stageBinDir.ensureDir();
await composeAppDir.copy(stageLibexecDir);

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

await patchInterpreters(stageLibexecDir);
await copyResolvedSharedObjects(stageLibexecDir, stageUsrDir.join("lib"));
await $`find ${stageAppDir} -type f -print0 | xargs -0 chmod u+w`;

await outputDir.ensureDir();
if (await outputPath.exists()) {
  await outputPath.remove();
}
if (await tarballPath.exists()) {
  await tarballPath.remove();
}

// FIXME: use bsdtar + lzma2 instead
await $`tar -czf ${tarballPath} -C ${stageUsrDir} .`;

const outputValue = outputPath.toString();
await $`appimage-run ${linuxdeployToolPath}
  --appdir ${stageAppDir}
  --desktop-file ${stageDesktopPath}
  --icon-file ${stageIconPath}
  --icon-filename ${desktopId}
  --deploy-deps-only ${stagedMainExecutablePath}
  --deploy-deps-only ${stageLibexecDir.join("lib")}
  --deploy-deps-only ${stageLibexecDir.join("lib", "app")}
  --deploy-deps-only ${stageLibexecDir.join("lib", "runtime", "lib")}
  --output appimage`.env({
  LDAI_OUTPUT: outputValue,
  OUTPUT: outputValue,
  LD_LIBRARY_PATH: "",
  DYLD_LIBRARY_PATH: "",
});

const dynamicSummary = await $`readelf -d ${stagedMainExecutablePath}`.text();
if (pathContainsNixStore(dynamicSummary)) {
  throw new Error(
    `staged executable still has /nix/store refs: ${stagedMainExecutablePath}`,
  );
}

console.log(`Created tarball: ${tarballPath}`);
console.log(`Created AppImage: ${outputPath}`);
