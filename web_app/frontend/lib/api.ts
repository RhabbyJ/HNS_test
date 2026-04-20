import { MateResponse, PartDetail, SearchResponse } from "@/lib/types";

const API_BASE_URL =
  process.env.HARNESSMATE_API_BASE_URL ?? "http://127.0.0.1:8000";

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }

  return (await response.json()) as T;
}

export async function searchParts(params: URLSearchParams): Promise<SearchResponse> {
  const query = params.toString();
  return fetchJson<SearchResponse>(`/search${query ? `?${query}` : ""}`);
}

export async function getPart(partId: string): Promise<PartDetail> {
  return fetchJson<PartDetail>(`/parts/${partId}`);
}

export async function getGroupedMates(partId: string): Promise<MateResponse> {
  return fetchJson<MateResponse>(`/parts/${partId}/mates`);
}
