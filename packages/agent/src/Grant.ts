// Platform grants, declared next to the workspace: `Workspace.layer({ grants: [Grant.read(Editor.address("desk"), "shared/desk/")] })`.
// Only the platform grants access to `shared/`; agents share from their own homes with `Workspace.share`.
import type { Address } from "./internal/core.ts";
import type { Grant } from "./internal/workspace.ts";

export type { Grant };

/** Read access to `path` and everything under it. */
export const read = (grantee: Address, path: string): Grant => ({ grantee: { type: grantee.type, name: grantee.name }, path, access: "read" });

/** Read and write access to `path` and everything under it. */
export const write = (grantee: Address, path: string): Grant => ({ grantee: { type: grantee.type, name: grantee.name }, path, access: "write" });
