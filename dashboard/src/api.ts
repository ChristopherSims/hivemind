let csrf = "";
let authorizationHandler: ((error: ApiError) => void) | null = null;

export function setCsrf(value: string) { csrf = value; }
export function setAuthorizationHandler(handler: ((error: ApiError) => void) | null) {
  authorizationHandler = handler;
}
export class ApiError extends Error {
  constructor(public status: number, message: string, public code?: string) { super(`${status}: ${message}`); }
}

export async function api<T>(path: string, method = "GET", body?: unknown, token?: string, idempotencyKey?: string): Promise<T> {
  // Capture the request's access context, not the handler of a later project/session.
  const onAuthorizationError = authorizationHandler;
  const response = await fetch(`/v1/${path}`, {
    method, credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
      ...(csrf ? {"X-CSRF-Token": csrf} : {}),
      ...(token ? {Authorization: `Bearer ${token}`} : {}),
      ...(method !== "GET" ? {"Idempotency-Key": idempotencyKey ?? crypto.randomUUID()} : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  let result;
  try { result = await response.json(); }
  catch (error) { if (response.ok) throw error; result = {}; }
  if (!response.ok) {
    const error = new ApiError(response.status, result.error?.message ?? "Request failed", result.error?.code);
    if (response.status === 401 || response.status === 403) onAuthorizationError?.(error);
    throw error;
  }
  return result.data as T;
}
