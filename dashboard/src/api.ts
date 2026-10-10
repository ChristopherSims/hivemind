let csrf = "";

export function setCsrf(value: string) { csrf = value; }

export async function api<T>(path: string, method = "GET", body?: unknown, token?: string): Promise<T> {
  const response = await fetch(`/v0.1/${path}`, {
    method, credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
      ...(csrf ? {"X-CSRF-Token": csrf} : {}),
      ...(token ? {Authorization: `Bearer ${token}`} : {}),
      ...(method !== "GET" ? {"Idempotency-Key": crypto.randomUUID()} : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(`${response.status}: ${result.error?.message ?? "Request failed"}`);
  return result.data as T;
}
