export type RegistrationMode = "open" | "invite" | "closed";

export const REGISTRATION_DISABLED_MESSAGE = "Registration is currently disabled";
export const REGISTRATION_INVITE_REQUIRED_MESSAGE = "Registration requires a valid invite";

export function getRegistrationMode(env: NodeJS.ProcessEnv = process.env): RegistrationMode {
  const configured = env.REGISTRATION_MODE?.trim().toLowerCase();
  if (!configured || configured === "open") return "open";
  if (configured === "invite") return "invite";
  if (configured === "closed") return "closed";
  // An invalid operator value must not accidentally open registration.
  return "closed";
}

export function getRegistrationBlockedReason(
  options: { hasValidInvite?: boolean } = {},
): string | null {
  const mode = getRegistrationMode();
  if (mode === "open") return null;
  if (mode === "invite" && options.hasValidInvite) return null;
  if (mode === "invite") return REGISTRATION_INVITE_REQUIRED_MESSAGE;
  return REGISTRATION_DISABLED_MESSAGE;
}

export function isRegistrationEnabled(options: { hasValidInvite?: boolean } = {}): boolean {
  return getRegistrationBlockedReason(options) === null;
}

export function assertRegistrationEnabled(options: { hasValidInvite?: boolean } = {}): void {
  const blockedReason = getRegistrationBlockedReason(options);
  if (blockedReason) {
    throw new Error(blockedReason);
  }
}
