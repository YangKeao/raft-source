import assert from "node:assert/strict";
import {
  assertRegistrationEnabled,
  getRegistrationBlockedReason,
  getRegistrationMode,
  REGISTRATION_DISABLED_MESSAGE,
  REGISTRATION_INVITE_REQUIRED_MESSAGE,
} from "./registrationPolicy";

function withRegistrationMode(value: string | undefined, run: () => void) {
  const previous = process.env.REGISTRATION_MODE;
  if (value === undefined) delete process.env.REGISTRATION_MODE;
  else process.env.REGISTRATION_MODE = value;
  try {
    run();
  } finally {
    if (previous === undefined) delete process.env.REGISTRATION_MODE;
    else process.env.REGISTRATION_MODE = previous;
  }
}

test("registration policy allows staging", () => {
  const previousDeploymentEnv = process.env.DEPLOYMENT_ENV;
  process.env.DEPLOYMENT_ENV = "staging";

  assert.equal(getRegistrationBlockedReason(), null);
  assert.doesNotThrow(() => assertRegistrationEnabled());

  if (previousDeploymentEnv === undefined) {
    delete process.env.DEPLOYMENT_ENV;
  } else {
    process.env.DEPLOYMENT_ENV = previousDeploymentEnv;
  }
});

test("registration policy allows production", () => {
  const previousDeploymentEnv = process.env.DEPLOYMENT_ENV;
  process.env.DEPLOYMENT_ENV = "production";

  assert.equal(getRegistrationBlockedReason(), null);
  assert.doesNotThrow(() => assertRegistrationEnabled());

  if (previousDeploymentEnv === undefined) {
    delete process.env.DEPLOYMENT_ENV;
  } else {
    process.env.DEPLOYMENT_ENV = previousDeploymentEnv;
  }
});

test("registration policy defaults to open", () => {
  withRegistrationMode(undefined, () => {
    assert.equal(getRegistrationMode(), "open");
    assert.equal(getRegistrationBlockedReason(), null);
  });
});

test("invite mode requires a server-validated invite", () => {
  withRegistrationMode(" invite ", () => {
    assert.equal(getRegistrationMode(), "invite");
    assert.equal(getRegistrationBlockedReason(), REGISTRATION_INVITE_REQUIRED_MESSAGE);
    assert.throws(() => assertRegistrationEnabled(), new RegExp(REGISTRATION_INVITE_REQUIRED_MESSAGE));
    assert.equal(getRegistrationBlockedReason({ hasValidInvite: true }), null);
    assert.doesNotThrow(() => assertRegistrationEnabled({ hasValidInvite: true }));
  });
});

test("closed and invalid modes fail closed", () => {
  for (const value of ["closed", "typo"]) {
    withRegistrationMode(value, () => {
      assert.equal(getRegistrationMode(), "closed");
      assert.equal(getRegistrationBlockedReason({ hasValidInvite: true }), REGISTRATION_DISABLED_MESSAGE);
    });
  }
});
