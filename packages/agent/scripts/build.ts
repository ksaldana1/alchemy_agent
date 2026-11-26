// Builds the npm package into dist/: compiled JS + .d.ts, the sources (for source maps), and a
// package.json whose exports point at the compiled files. The workspace package.json keeps
// pointing at src/ so the examples and tests run without a build. Publish with `npm publish ./dist`.
import { $, Glob } from "bun";

await $`rm -rf dist`;
await $`tsc -p tsconfig.build.json`;

// tsc rewrites relative `.ts` imports in JS output but not in declarations.
const specifier = /((?:from |import\()["']\.{1,2}\/[^"']*?)\.ts(["'])/g;
for await (const file of new Glob("dist/**/*.d.ts").scan(".")) {
  const text = await Bun.file(file).text();
  const next = text.replace(specifier, "$1.js$2");
  if (next !== text) await Bun.write(file, next);
}

// Maps point at packages/agent/src; the published package carries the sources at dist/src.
for await (const file of new Glob("dist/**/*.map").scan(".")) {
  const map = await Bun.file(file).json();
  map.sources = map.sources.map((source: string) => source.replace(/^\.\.\//, ""));
  await Bun.write(file, JSON.stringify(map));
}

const pkg = await Bun.file("package.json").json();
const entry = (name: string) => ({ types: `./${name}.d.ts`, default: `./${name}.js` });
const { private: _, scripts: __, devDependencies: ___, ...published } = pkg;
await Bun.write(
  "dist/package.json",
  JSON.stringify({ ...published, exports: { ".": entry("index"), "./testing": entry("Testing"), "./package.json": "./package.json" } }, null, 2) + "\n",
);
await $`cp -R src dist/src`;
await $`cp ../../README.md ../../LICENSE dist/`;
