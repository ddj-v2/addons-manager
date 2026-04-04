import {
    Context, Handler, PRIV, Schema, 
    Service, superagent, SystemModel, 
    TokenModel, 
    UserFacingError, ForbiddenError, Types,
    Model,
    requireSudo
} from 'hydrooj';
import { spawn } from 'child_process';
import * as fs from 'fs/promises';
import * as path from 'path';
import semver from 'semver';
import validatePackageName from 'validate-npm-package-name';

// Constants
const ADDON_JSON = 'addon.json';
const ADDON_LOCKED_JSON = 'addon-locked.json';
const ADDONS_DIR = 'addons/';
const TEMPLATE_NAME = 'manage_addons.html';
const MARKET_TEMPLATE_NAME = 'manage_addons_market.html';
const ROUTE_PATH = '/manage/addons';
const MARKET_ROUTE_PATH = '/manage/addons/market';
const DEFAULT_BRANCH = 'main';
const LOG_PREFIX = '[Addons Manager]';

// Types
type CommandResult = { success: boolean; message?: string };
type PackageInfo = { name: string; version: string };
type PackageAction = 'add' | 'delete' | 'update';
type PackageOperation = PackageInfo & { action: PackageAction };
type ManagerResponse = {
    packages: string[];
    lockedPackages: string[];
    success: boolean | null;
    result: string | null;
};

type AddonsManagerModel = {
    manageAddon: (action: PackageAction, name: string, version?: string) => Promise<CommandResult>;
    localUpdate: (name: string) => Promise<CommandResult>;
    localDelete: (name: string) => Promise<CommandResult>;
    localAdd: (name: string) => Promise<CommandResult>;
    getActivedPackages: () => Promise<string[]>;
    getLockedPackages: () => Promise<string[]>;
    localPackageName: (name: string) => string;
};

type AddonEntry = {
    title: string;
    description: string;
    npmPackage: string | null;
    gitUrl: string | null;
    author: string;
    url: string;
    upvotes: number;
};

// Validators
const PackageValidator = {
    checkNpmPackageName(name: string): boolean {
        return validatePackageName(name).validForNewPackages;
    },
    checkNpmVersion(version: string): boolean {
        return version === '' || semver.valid(version) !== null;
    },
    checkPackageIsGitUrl(name: string): boolean {
        return name.endsWith('.git') && (name.startsWith('http://') || name.startsWith('https://') || name.startsWith('git@'));
    },
    checkPackageIsLocal(name: string): boolean {
        return (name[0] === '/' && path.isAbsolute(name)) || this.checkPackageIsGitUrl(name);
    }
};
class AddonsManagerHandler extends Handler {
    private static model: AddonsManagerModel;

    static setModel(m: AddonsManagerModel) { 
        this.model = m; 
    }

    private async getManagerResponse(): Promise<ManagerResponse> {
        const packages = await AddonsManagerHandler.model.getActivedPackages();
        const lockedPackages = await AddonsManagerHandler.model.getLockedPackages();
        return {
            packages,
            lockedPackages,
            success: null,
            result: null
        };
    }

    @requireSudo
    async get() {
        this.response.template = TEMPLATE_NAME;
        const response = await this.getManagerResponse();
        this.response.body = this.request.body || response;
        this.renderHTML(this.response.template, { title: 'manage_addons' });
    }

    async post() {
        const body = this.request.body;
        const pkg: PackageOperation = {
            name: body['package_name'],
            version: body['package_version'] || '',
            action: body['action']
        };

        const result = await this.handlePackageOperation(pkg);
        this.logOperation(pkg, result);

        if (result.success) {
            const response = await this.getManagerResponse();
            this.response.template = TEMPLATE_NAME;
            this.response.body = {
                ...response,
                success: result.success,
                result: result.message || 'Operation successful'
            };
            this.renderHTML(this.response.template, { title: 'manage_addons' });
        } else {
            throw new UserFacingError(result.message || 'Operation failed');
        }
    }

    private async handlePackageOperation(pkg: PackageOperation): Promise<CommandResult> {
        const packages = await AddonsManagerHandler.model.getActivedPackages();
        const lockedPackages = await AddonsManagerHandler.model.getLockedPackages();

        const isInstalled = packages.includes(pkg.name) || 
                          packages.includes(AddonsManagerHandler.model.localPackageName(pkg.name));
        const isLocked = lockedPackages.includes(pkg.name);

        return PackageValidator.checkPackageIsLocal(pkg.name)
            ? this.handleLocalPackageOperation(pkg, isInstalled, isLocked)
            : this.handleRemotePackageOperation(pkg, isInstalled, isLocked);
    }

    private async handleLocalPackageOperation(pkg: PackageOperation, isInstalled: boolean, isLocked: boolean): Promise<CommandResult> {
        const localName = AddonsManagerHandler.model.localPackageName(pkg.name);
        
        if (localName === '') {
            return { success: false, message: 'Invalid local package path.' };
        }

        switch (pkg.action) {
            case 'add':
                return AddonsManagerHandler.model.localAdd(pkg.name);
            case 'update':
                if (isInstalled) {
                    return AddonsManagerHandler.model.localUpdate(pkg.name);
                }
                return { success: false, message: 'Package is already installed. Use local update instead.' };
            case 'delete':
                return AddonsManagerHandler.model.localDelete(pkg.name);
            default:
                return { success: false, message: 'Unknown action' };
        }
    }

    private async handleRemotePackageOperation(
        pkg: PackageOperation, 
        isInstalled: boolean, 
        isLocked: boolean
    ): Promise<CommandResult> {
        switch (pkg.action) {
            case 'add':
                if (isInstalled) {
                    return { success: false, message: 'Package is already installed' };
                }
                break;
            case 'update':
            case 'delete':
                if (!isInstalled) {
                    return { success: false, message: 'Package is not installed' };
                }
                break;
        }

        if (pkg.action === 'delete' && isLocked) {
            return { success: false, message: 'This package is locked and cannot be removed.' };
        }

        return AddonsManagerHandler.model.manageAddon(pkg.action, pkg.name, pkg.version);
    }

    private logOperation(pkg: PackageOperation, result: CommandResult): void {
        console.log(
            `${LOG_PREFIX} Action=${pkg.action} Package=${pkg.name} Version=${pkg.version} Result=${JSON.stringify(result)}`
        );
    }
}

function parseAddonEntry(discussion: any): AddonEntry | null {
    const { title, body, html_url, user, upvote_count } = discussion;
    const npmMatch = (body || '').match(/^npm:\s*(.+)$/m);
    const gitMatch = (body || '').match(/^git:\s*(.+)$/m);
    const npmPackage = npmMatch ? npmMatch[1].trim() : null;
    const gitUrl = gitMatch ? gitMatch[1].trim() : null;
    if (!npmPackage && !gitUrl) return null;
    const description = (body || '')
        .split('\n')
        .filter((line: string) => !line.match(/^(npm|git):\s*/))
        .join('\n')
        .trim();
    return {
        title,
        description,
        npmPackage,
        gitUrl,
        author: user?.login || 'unknown',
        url: html_url,
        upvotes: upvote_count || 0,
    };
}

async function fetchMarketAddons(owner: string, repo: string, limit: number): Promise<AddonEntry[]> {
    const url = `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/discussions?per_page=${limit}&sort=updated&direction=desc`;
    const res = await superagent
        .get(url)
        .set('Accept', 'application/vnd.github+json')
        .set('User-Agent', 'HydroOJ-Addons-Manager')
        .set('X-GitHub-Api-Version', '2022-11-28');
    const nodes: any[] = Array.isArray(res.body) ? res.body : [];
    return nodes.map(parseAddonEntry).filter((e): e is AddonEntry => e !== null);
}

class AddonsMarketHandler extends Handler {
    private static marketOwner: string;
    private static marketRepo: string;
    private static fetchLimit: number;

    static setConfig(owner: string, repo: string, fetchLimit: number) {
        this.marketOwner = owner;
        this.marketRepo = repo;
        this.fetchLimit = Math.min(100, Math.max(1, fetchLimit));
    }

    @requireSudo
    async get() {
        this.response.template = MARKET_TEMPLATE_NAME;
        let addons: AddonEntry[] = [];
        let error: string | null = null;
        try {
            addons = await fetchMarketAddons(
                AddonsMarketHandler.marketOwner,
                AddonsMarketHandler.marketRepo,
                AddonsMarketHandler.fetchLimit,
            );
        } catch (err) {
            error = err instanceof Error ? err.message : 'failed to fetch addon market';
            console.error(`${LOG_PREFIX} Market fetch error:`, err);
        }
        this.response.body = { addons, error, marketOwner: AddonsMarketHandler.marketOwner, marketRepo: AddonsMarketHandler.marketRepo };
        this.renderHTML(this.response.template, { title: 'addon market' });
    }
}

async function sendCommand(command: string, args: string[], cwd: string): Promise<CommandResult> {
    return new Promise((resolve) => {
        const child = spawn(command, args, { cwd });
        let stdout = '';
        let stderr = '';

        child.stdout?.on('data', (data) => { stdout += data.toString(); });
        child.stderr?.on('data', (data) => { stderr += data.toString(); });
        
        child.on('error', (error) => {
            resolve({ success: false, message: error.message });
        });

        child.on('close', (code) => {
            if (code === 0) {
                resolve({ success: true, message: stdout || stderr });
            } else {
                resolve({
                    success: false,
                    message: stderr || stdout || `Exit code ${code}`
                });
            }
        });
    });
}

export default class AddonsManagerService extends Service {
    static Config = Schema.object({
        pathToHydro: Schema.string().description('Path to Hydro').required(),
        marketGithubOwner: Schema.string().description('GitHub owner for addon market discussions').default('Bryan0324'),
        marketGithubRepo: Schema.string().description('GitHub repo for addon market discussions').default('hydrooj-addons-manager'),
        marketFetchLimit: Schema.number().description('Max number of addon discussions to fetch from GitHub (1–100)').default(100),
    });

    constructor(ctx: Context, config: ReturnType<typeof AddonsManagerService.Config>) {
        super(ctx, 'hydrooj-addons-manager');
        ctx.Route('manage_addons', ROUTE_PATH, AddonsManagerHandler, PRIV.PRIV_ALL);
        ctx.Route('manage_addons_market', MARKET_ROUTE_PATH, AddonsMarketHandler, PRIV.PRIV_ALL);
        global.Hydro.ui.inject('ControlPanel', 'manage_addons');
        this.initialize(ctx, config);
    }

    private initialize(ctx: Context, config: ReturnType<typeof AddonsManagerService.Config>): void {
        const model = this.createModel(config);
        AddonsManagerHandler.setModel(model);
        AddonsMarketHandler.setConfig(
            config.marketGithubOwner,
            config.marketGithubRepo,
            config.marketFetchLimit,
        );
    }

    private createModel(config: ReturnType<typeof AddonsManagerService.Config>): AddonsManagerModel {
        return {
            manageAddon: (action, name, version = '') => this.manageAddon(action, name, version, config),
            localUpdate: (name) => this.localUpdate(name, config),
            localDelete: (name) => this.localDelete(name, config),
            localAdd: (name) => this.localAdd(name, config),
            getActivedPackages: () => this.getActivedPackages(config),
            getLockedPackages: () => this.getLockedPackages(config),
            localPackageName: (name) => this.extractLocalPackageName(name, config),
        };
    }

    private async manageAddon(
        action: PackageAction, 
        name: string, 
        version: string = '',
        config: ReturnType<typeof AddonsManagerService.Config>
    ): Promise<CommandResult> {
        if (!PackageValidator.checkNpmPackageName(name)) {
            return { success: false, message: 'Invalid package name' };
        }
        if (version !== '' && !PackageValidator.checkNpmVersion(version)) {
            return { success: false, message: 'Invalid version' };
        }

        switch (action) {
            case 'delete':
                return this.deleteRemotePackage(name, config);
            case 'update':
                return this.updateRemotePackage(name, version, config);
            case 'add':
                return this.addRemotePackage(name, version, config);
            default:
                return { success: false, message: 'Unknown action' };
        }
    }

    private async deleteRemotePackage(name: string, config: ReturnType<typeof AddonsManagerService.Config>): Promise<CommandResult> {
        const yarnResult = await sendCommand('yarn', ['global', 'remove', name], config.pathToHydro);
        if(!yarnResult.success) return yarnResult;
        const result = await sendCommand('hydrooj', ['addon', 'remove', name], config.pathToHydro);
        return { success: result.success, message: (yarnResult.message || '') + result.message };
    }

    private async updateRemotePackage(
        name: string, 
        version: string,
        config: ReturnType<typeof AddonsManagerService.Config>
    ): Promise<CommandResult> {
        const packageSpec = version ? `${name}@${version}` : name;
        return sendCommand('yarn', ['global', 'upgrade', packageSpec, '--latest'], config.pathToHydro);
    }

    private async addRemotePackage(
        name: string, 
        version: string,
        config: ReturnType<typeof AddonsManagerService.Config>
    ): Promise<CommandResult> {
        const packageSpec = version ? `${name}@${version}` : name;
        const yarnResult = await sendCommand('yarn', ['global', 'add', packageSpec], config.pathToHydro);
        if(!yarnResult.success) return yarnResult;
        const result = await sendCommand('hydrooj', ['addon', 'add', name], config.pathToHydro);
        return { success: result.success, message: (yarnResult.message || '') + result.message };
    }

    private async localUpdate(name: string, config: ReturnType<typeof AddonsManagerService.Config>): Promise<CommandResult> {
        if (!PackageValidator.checkPackageIsLocal(name)) {
            return { success: false, message: 'Not a local package' };
        }
        const localPath = this.localPackagePath(name, config);
        return sendCommand('git', ['pull', 'origin', DEFAULT_BRANCH], localPath);
    }

    private async localAdd(name: string, config: ReturnType<typeof AddonsManagerService.Config>): Promise<CommandResult> {
        if (!PackageValidator.checkPackageIsLocal(name)) {
            return { success: false, message: 'Not a local package' };
        }
        const deleteResult = await this.localDelete(name, config);
        const gitResult = await sendCommand('git', ['clone', name], config.pathToHydro + ADDONS_DIR);
        if (!gitResult.success) {
            return gitResult;
        }
        const localPath = this.localPackagePath(name, config);
        const result = await sendCommand('hydrooj', ['addon', 'add', localPath], config.pathToHydro);
        return { success: result.success, message: (gitResult.message || '') + result.message };
    }

    private async localDelete(name: string, config: ReturnType<typeof AddonsManagerService.Config>): Promise<CommandResult> {
        if (!PackageValidator.checkPackageIsLocal(name)) {
            return { success: false, message: 'Not a local package' };
        }
        const localPath = this.localPackagePath(name, config);
        const deleteResult : CommandResult = await fs.rm(localPath, { recursive: true, force: true })
            .then(() => ({ success: true, message: 'Local package deleted successfully' }))
            .catch((err) => ({ success: false, message: err.message }));
        if (!deleteResult.success) {
            return deleteResult;
        }
        const result = await sendCommand('hydrooj', ['addon', 'remove', localPath], config.pathToHydro);
        return { success: result.success, message: (deleteResult.message || '') + result.message };
    }

    private async getActivedPackages(config: ReturnType<typeof AddonsManagerService.Config>): Promise<string[]> {
        try {
            const data = await fs.readFile(config.pathToHydro + ADDON_JSON, 'utf-8');
            return JSON.parse(data);
        } catch {
            return [];
        }
    }

    private async getLockedPackages(config: ReturnType<typeof AddonsManagerService.Config>): Promise<string[]> {
        try {
            const data = await fs.readFile(config.pathToHydro + ADDON_LOCKED_JSON, 'utf-8');
            return JSON.parse(data);
        } catch {
            return [];
        }
    }

    private extractLocalPackageName(name: string, config: ReturnType<typeof AddonsManagerService.Config>): string {
        if (!PackageValidator.checkPackageIsLocal(name)) return '';
        if(PackageValidator.checkPackageIsGitUrl(name)) {
            const match = name.match(/([^/]+?)(\.git)?$/);
            return match ? match[1] : '';
        }
        if(!name.startsWith(config.pathToHydro + ADDONS_DIR)) {
            return '';
        }
        name = name.replace(config.pathToHydro + ADDONS_DIR, '');
        return name ? name : '';
    }
    private localPackagePath(name: string, config: ReturnType<typeof AddonsManagerService.Config>): string {
        const localName = this.extractLocalPackageName(name, config);
        return path.join(config.pathToHydro, ADDONS_DIR, localName);
    }
}