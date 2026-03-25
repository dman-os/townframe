#!/usr/bin/env -S deno run --allow-all

// FIXME: provide a devshell for building onnxcore (python and so on)

import { $ } from "./utils.ts";

async function removeTreeIfExists(targetPath: string) {
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      await Deno.remove(targetPath, { recursive: true });
      return;
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) {
        return;
      }
      const message = error instanceof Error ? error.message : String(error);
      const isRetryable = message.includes("Directory not empty") ||
        message.includes("resource busy");
      if (!isRetryable || attempt === 4) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 200 * (attempt + 1)));
    }
  }
}

async function cleanupOrtBuildArtifacts(sourceDir: {
  join(path: string): { toString(): string };
}) {
  // Keep extracted ORT sources for reuse, but drop heavy generated Android build output.
  const androidBuildPath = `${sourceDir.join("build").toString()}/Android`;
  await removeTreeIfExists(androidBuildPath);
}

const abiToTriple = {
  "arm64-v8a": "aarch64-linux-android",
  "armeabi-v7a": "armv7-linux-androideabi",
  x86_64: "x86_64-linux-android",
  x86: "i686-linux-android",
} as const;

const abi = $.env.DAYBOOK_ANDROID_ABI ?? "arm64-v8a";
const triple = abiToTriple[abi as keyof typeof abiToTriple];
if (!triple) throw new Error(`Unsupported DAYBOOK_ANDROID_ABI=${abi}`);

const composeProfile = ($.env.DAYBOOK_COMPOSE_PROFILE ?? "debug").toLowerCase();
if (!(composeProfile === "debug" || composeProfile === "release")) {
  throw new Error(
    `Unsupported DAYBOOK_COMPOSE_PROFILE=${composeProfile}; expected debug or release`,
  );
}
const gradleVariant = composeProfile === "release" ? "Release" : "Debug";
const gradleTask = $.env.DAYBOOK_ANDROID_GRADLE_TASK ??
  `assemble${gradleVariant}`;
const ortBuildConfig = $.env.ORT_BUILD_CONFIG ??
  (composeProfile === "release" ? "Release" : "Debug");

const ortSourceTag = $.env.ORT_SOURCE_TAG ?? "v1.24.1";
const androidApiLevel = $.env.ANDROID_API_LEVEL ?? "31";
const androidNdkRoot = $.env.ANDROID_NDK_ROOT;
if (!androidNdkRoot) throw new Error("ANDROID_NDK_ROOT must be set");

const ortRootDir = $.relativeDir("../target/ort");
const sourceArchivePath = ortRootDir.join(`onnxruntime-${ortSourceTag}.tar.gz`);
const sourceDir = ortRootDir.join(`onnxruntime-src-${ortSourceTag}`);
const sourceCompleteFile = ortRootDir.join(`.source-${ortSourceTag}.complete`);
const distDir = ortRootDir.join("dist", ortSourceTag, triple, ortBuildConfig);
const distCompleteFile = ortRootDir.join(
  `.dist-${ortSourceTag}-${triple}-${ortBuildConfig.toLowerCase()}.complete`,
);
const libDirFile = ortRootDir.join(
  `ort-lib-location-${ortSourceTag}-${triple}-${ortBuildConfig.toLowerCase()}.txt`,
);

await ortRootDir.ensureDir();

if (!(await distCompleteFile.exists())) {
  const needsSourceExtract = !(await sourceDir.exists());
  if (!(await sourceCompleteFile.exists()) || needsSourceExtract) {
    if (!(await sourceArchivePath.exists())) {
      await $.request(
        `https://github.com/microsoft/onnxruntime/archive/refs/tags/${ortSourceTag}.tar.gz`,
      )
        .showProgress()
        .pipeToPath(sourceArchivePath);
    }
    await removeTreeIfExists(sourceDir.toString());
    await sourceDir.ensureDir();
    await $`bsdtar --extract --file ${sourceArchivePath} --directory ${sourceDir} --strip-components=1`;
    await sourceCompleteFile.writeText("ok\n");
  }

  await $`bash ./build.sh --update --build --config ${ortBuildConfig} --parallel --compile_no_warning_as_error --skip_submodule_sync --build_shared_lib --android --android_abi=${abi} --android_api=${androidApiLevel} --android_ndk_path=${androidNdkRoot}`
    .cwd(
      sourceDir,
    );
  const builtLibDir = sourceDir.join("build", "Android", ortBuildConfig);
  const sharedLibPathsRaw = await $`find ${builtLibDir} -type f -name '*.so*'`
    .text();
  const sharedLibPaths = sharedLibPathsRaw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (sharedLibPaths.length === 0) {
    throw new Error(
      `ORT build did not produce shared libraries under ${builtLibDir}`,
    );
  }
  await removeTreeIfExists(distDir.toString());
  await distDir.ensureDir();
  for (const sourceLibPath of sharedLibPaths) {
    await $`cp ${sourceLibPath} ${distDir}`;
  }
  await libDirFile.writeText(`${distDir}\n`);
  await distCompleteFile.writeText("ok\n");
}

if (await sourceDir.exists()) {
  await cleanupOrtBuildArtifacts(sourceDir);
}

await $`./gradlew ${gradleTask} -PdaybookProfile=${composeProfile}`
  .cwd($.relativeDir("../src/daybook_compose/"))
  .env({
    ORT_LIB_LOCATION: (await libDirFile.readText()).trim(),
    ORT_LIB_PROFILE: $.env.ORT_LIB_PROFILE ?? ortBuildConfig,
    ORT_PREFER_DYNAMIC_LINK: $.env.ORT_PREFER_DYNAMIC_LINK ?? "1",
  });
