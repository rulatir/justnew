#!/usr/bin/env node
import { Command } from "commander";
import { globby } from "globby";
import xxhash from "xxhash-wasm";
import fs from "fs/promises";
import path from "path";
import os from "os";
import pLimit from "p-limit";

const program = new Command();

program
    .name("justnew")
    .description("Delete duplicate files from <newDir> that also exist (by content) in <oldDir>")
    .argument("<oldDir>", "reference directory")
    .argument("<newDir>", "directory to clean")
    .option("-d, --dry-run", "only show what would be deleted")
    .option("-v, --verbose", "log progress and status to stderr")
    .option("-n, --match-names", "use [filename, size] as preliminary match key")
    .option("-p, --match-paths", "use [relative path, size] as preliminary match key")
    .option("--concurrency <n>", "number of parallel operations", value => parseInt(value, 10))
    .parse();

const options = program.opts();
const [oldDirInput, newDirInput] = program.args;

if (!oldDirInput || !newDirInput) {
    console.error("Error: Both oldDir and newDir are required.");
    process.exit(1);
}

const verbose = !!options["verbose"];
const dryRun = !!options["dryRun"];
const matchNames = !!options["matchNames"];
const matchPaths = !!options["matchPaths"];
const concurrency = options["concurrency"] ?? os.cpus().length;
const limit = pLimit(concurrency);

function logStage(message: string): void {
    if (verbose) console.error(message);
}

type Phase1Key = string;
type Phase1Map = Map<Phase1Key, string[]>;
type FileHashMap = Map<string, string[]>;

function computePreliminaryKey(absPath: string, baseDir: string, size: number): string {
    if (matchPaths) {
        const rel = path.relative(baseDir, absPath);
        return JSON.stringify([rel, size]);
    } else if (matchNames) {
        return JSON.stringify([path.basename(absPath), size]);
    } else {
        return size.toString();
    }
}

async function getAllFileSizes(files: string[]): Promise<Map<string, number>> {
    const entries = await Promise.all(
        files.map(async f => {
            const size = (await fs.stat(f)).size;
            return [f, size] as const;
        })
    );
    return new Map(entries);
}

async function processFileForGrouping(file: string, dir: string, map: Phase1Map, label: string, processedCounter: { count: number }, total: number): Promise<void> {
    const stat = await fs.stat(file);
    const key = computePreliminaryKey(file, dir, stat.size);
    const list = map.get(key);
    if (list) {
        list.push(file);
    } else {
        map.set(key, [file]);
    }

    if (verbose) {
        processedCounter.count++;
        if (processedCounter.count % 100 === 0 || processedCounter.count === total) {
            console.error(`[${label}] ${processedCounter.count}/${total} grouped`);
        }
    }
}

async function getPreliminaryMap(dir: string, label: string): Promise<Phase1Map> {
    const files = await globby(["**/*"], { cwd: dir, absolute: true, onlyFiles: true });
    const map: Phase1Map = new Map();
    const processed = { count: 0 };

    logStage(`Grouping files in ${label} by preliminary key (${files.length} files)`);

    await Promise.all(files.map(file => limit(() => processFileForGrouping(file, dir, map, label, processed, files.length))));

    logStage(`Finished grouping ${label} (${map.size} key buckets)`);
    return map;
}

function hashToHex(hashBigInt: bigint): string {
    return hashBigInt.toString(16).padStart(16, "0");
}

class GlobalHashingState {
    processed = 0;
    hashedBytes = 0;
    dupes = 0;
    totalJobFiles = 0;
    totalBytes = 0;

    constructor(totalJobFiles: number, totalBytes: number) {
        this.totalJobFiles = totalJobFiles;
        this.totalBytes = totalBytes;
    }

    logProgress(file: string): void {
        if (!verbose) return;
        const lines = [
            `Hashed: ${file}`,
            `Files: ${this.processed} of ${this.totalJobFiles}`,
            `Bytes: ${this.hashedBytes} of ${this.totalBytes}`,
            `Dupes: ${this.dupes} of ${this.totalJobFiles}`
        ];
        process.stderr.write("\x1b[4F" + lines.map(line => `\x1b[0K${line}`).join("\n") + "\n");
    }
}

class HashingContext {
    readonly hasher: (data: Uint8Array) => bigint;
    readonly matchMap?: Set<string>;
    readonly fileMap: FileHashMap = new Map();
    readonly state: GlobalHashingState;

    constructor(
        hasher: (data: Uint8Array) => bigint,
        state: GlobalHashingState,
        matchMap?: Set<string>
    ) {
        this.hasher = hasher;
        this.matchMap = matchMap;
        this.state = state;
    }

    async processFile(file: string, preKey: string): Promise<void> {
        if (verbose && this.state.processed === 0) {
            this.state.logProgress(file);
        }

        const buf = await fs.readFile(file);
        const hash = hashToHex(this.hasher(buf));
        const key = `${preKey}:${hash}`;
        const list = this.fileMap.get(key);
        if (list) {
            list.push(file);
        } else {
            this.fileMap.set(key, [file]);
        }
        this.state.hashedBytes += buf.length;
        if (this.matchMap?.has(key)) this.state.dupes++;

        this.state.processed++;
        this.state.logProgress(file);
    }
}

async function hashGroup(
    files: string[],
    preKey: string,
    context: HashingContext
): Promise<FileHashMap> {
    await Promise.all(files.map(file => limit(() => context.processFile(file, preKey))));
    return context.fileMap;
}

async function dedupe(oldDir: string, newDir: string): Promise<void> {
    logStage(`Using concurrency: ${concurrency}`);
    if (dryRun) logStage("Dry run enabled — no files will be deleted.");

    const hasher = await xxhash();

    const [oldGroups, newGroups] = await Promise.all([
        getPreliminaryMap(oldDir, "old directory"),
        getPreliminaryMap(newDir, "new directory")
    ]);

    const intersectingKeys = [...oldGroups.keys()].filter(key => newGroups.has(key));
    logStage(`Found ${intersectingKeys.length} overlapping buckets`);

    const oldHashMap: FileHashMap = new Map();
    const newHashMap: FileHashMap = new Map();
    const allOldFiles = intersectingKeys.flatMap(k => oldGroups.get(k) ?? []);
    const allNewFiles = intersectingKeys.flatMap(k => newGroups.get(k) ?? []);
    const totalJobFiles = allOldFiles.length + allNewFiles.length;

    const fileSizes = await getAllFileSizes([...allOldFiles, ...allNewFiles]);
    const totalBytes = [...fileSizes.values()].reduce((a, b) => a + b, 0);

    const globalState = new GlobalHashingState(totalJobFiles, totalBytes);

    for (const key of intersectingKeys) {
        const oldGroup = oldGroups.get(key);
        const newGroup = newGroups.get(key);
        if (!oldGroup || !newGroup) continue;

        const oldCtx = new HashingContext(data => hasher.h64Raw(data), globalState);
        const oldHashes = await hashGroup(oldGroup, key, oldCtx);
        for (const [hkey, paths] of oldHashes) {
            const list = oldHashMap.get(hkey);
            if (list) list.push(...paths);
            else oldHashMap.set(hkey, [...paths]);
        }

        const knownHashes = new Set(oldHashMap.keys());
        const newCtx = new HashingContext(data => hasher.h64Raw(data), globalState, knownHashes);
        const newHashes = await hashGroup(newGroup, key, newCtx);
        for (const [hkey, paths] of newHashes) {
            const list = newHashMap.get(hkey);
            if (list) list.push(...paths);
            else newHashMap.set(hkey, [...paths]);
        }
    }

    const duplicateKeys = [...oldHashMap.keys()].filter(k => newHashMap.has(k));
    duplicateKeys.sort((a, b) => {
        const aList = oldHashMap.get(a);
        const bList = oldHashMap.get(b);
        if (!aList || !bList) return 0;
        return aList[0].localeCompare(bList[0]);
    });

    for (const key of duplicateKeys) {
        const oldFiles = oldHashMap.get(key);
        const newFiles = newHashMap.get(key);
        if (!oldFiles || !newFiles) continue;

        oldFiles.sort();
        newFiles.sort();

        for (const file of oldFiles) console.log(file);
        for (const file of newFiles) console.log("  " + file);
        console.log();

        for (const newFile of newFiles) {
            if (!dryRun) {
                await fs.unlink(newFile);
                if (!verbose) console.log(`Deleted: ${newFile}`);
            }
        }
    }

    if (!verbose) console.log("Done.");
}

await dedupe(path.resolve(oldDirInput), path.resolve(newDirInput)).catch(err => {
    console.error("Fatal error:", err);
    process.exit(1);
});
