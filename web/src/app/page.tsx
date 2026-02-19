"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { apiFetch } from "@/lib/api";
import { Building } from "@/lib/types";

export default function Home() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<Building[]>([]);
  const [loading, setLoading] = useState(false);
  const router = useRouter();

  async function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    if (!query.trim()) return;
    setLoading(true);
    try {
      const data = await apiFetch<{ results: Building[] }>(
        `/api/buildings/search?q=${encodeURIComponent(query)}`
      );
      setResults(data.results);
    } catch {
      setResults([]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-[calc(100vh-3.5rem)] flex flex-col items-center justify-center px-4">
      <div className="text-center mb-12">
        <h1 className="text-5xl font-bold tracking-tight mb-3">Vayo</h1>
        <p className="text-zinc-400 text-lg">
          NYC Real Estate Intelligence
        </p>
      </div>

      <form onSubmit={handleSearch} className="w-full max-w-xl mb-8">
        <div className="flex gap-2">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search by address, e.g. 43 5 Avenue"
            className="flex-1 bg-zinc-900 border border-zinc-700 rounded-lg px-4 py-3 text-sm
                       placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-blue-500
                       focus:border-transparent"
          />
          <button
            type="submit"
            disabled={loading}
            className="bg-blue-600 hover:bg-blue-500 text-white px-6 py-3 rounded-lg text-sm
                       font-medium transition disabled:opacity-50"
          >
            {loading ? "..." : "Search"}
          </button>
        </div>
      </form>

      {results.length > 0 && (
        <div className="w-full max-w-xl">
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg overflow-hidden">
            {results.map((b) => (
              <button
                key={b.bbl}
                onClick={() => router.push(`/building/${b.bbl}`)}
                className="w-full text-left px-4 py-3 hover:bg-zinc-800 transition
                           border-b border-zinc-800 last:border-b-0"
              >
                <div className="font-medium text-sm">{b.address}</div>
                <div className="text-xs text-zinc-400 mt-0.5">
                  {b.borough} {b.zipcode} &middot;{" "}
                  {b.units_residential} units &middot;{" "}
                  Built {b.year_built || "?"} &middot;{" "}
                  {b.num_floors} floors
                </div>
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="mt-12 flex gap-6 text-sm text-zinc-500">
        <button
          onClick={() => router.push("/explorer")}
          className="hover:text-zinc-300 transition"
        >
          Market Explorer
        </button>
        <button
          onClick={() => router.push("/watchlist")}
          className="hover:text-zinc-300 transition"
        >
          Watchlist
        </button>
      </div>
    </div>
  );
}
