"use client";

import { useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import { apiFetch, apiPost, apiDelete } from "@/lib/api";
import { WatchlistItem, Alert, Building } from "@/lib/types";
import { AddressSearch } from "@/components/AddressSearch";

export default function WatchlistPage() {
  const router = useRouter();
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [w, a] = await Promise.all([
        apiFetch<WatchlistItem[]>("/api/watchlist"),
        apiFetch<Alert[]>("/api/alerts?limit=50"),
      ]);
      setWatchlist(w);
      setAlerts(a);
    } catch {
      // pass
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  async function handleAdd(building: Building) {
    await apiPost("/api/watchlist", { bbl: building.bbl });
    fetchData();
  }

  async function handleRemove(bbl: number) {
    await apiDelete(`/api/watchlist/${bbl}`);
    fetchData();
  }

  async function handleMarkRead(id: number) {
    await apiPost(`/api/alerts/${id}/read`, {});
    setAlerts((prev) =>
      prev.map((a) => (a.id === id ? { ...a, read: 1 } : a))
    );
  }

  return (
    <div className="max-w-4xl mx-auto px-4 py-8">
      <h1 className="text-xl font-bold mb-6">Watchlist</h1>

      {/* Add building */}
      <div className="mb-8">
        <label className="text-sm text-zinc-400 block mb-2">
          Add building to watchlist
        </label>
        <div className="max-w-md">
          <AddressSearch
            onSelect={handleAdd}
            placeholder="Search by address to add..."
          />
        </div>
      </div>

      {/* Watched buildings */}
      <div className="mb-10">
        <h2 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
          Watched Buildings ({watchlist.length})
        </h2>
        {loading ? (
          <p className="text-zinc-500 text-sm">Loading...</p>
        ) : watchlist.length === 0 ? (
          <p className="text-zinc-500 text-sm">
            No buildings watched yet. Search above to add one.
          </p>
        ) : (
          <div className="space-y-2">
            {watchlist.map((w) => (
              <div
                key={w.bbl}
                className="bg-zinc-900 border border-zinc-800 rounded-lg p-4 flex items-center
                           justify-between"
              >
                <button
                  onClick={() => router.push(`/building/${w.bbl}`)}
                  className="text-left"
                >
                  <div className="font-medium text-sm">{w.address}</div>
                  <div className="text-xs text-zinc-400 mt-0.5">
                    {w.borough} {w.zipcode} &middot;{" "}
                    {w.units_residential} units &middot;{" "}
                    Added{" "}
                    {w.added_at
                      ? new Date(w.added_at).toLocaleDateString()
                      : "?"}
                  </div>
                </button>
                <button
                  onClick={() => handleRemove(w.bbl)}
                  className="text-zinc-500 hover:text-red-400 text-xs px-2 py-1 transition"
                >
                  Remove
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Alerts */}
      <div>
        <h2 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
          Alerts ({alerts.filter((a) => !a.read).length} unread)
        </h2>
        {alerts.length === 0 ? (
          <p className="text-zinc-500 text-sm">
            No alerts yet. Alerts appear when watched buildings have changes
            (new listings, permits, ownership changes).
          </p>
        ) : (
          <div className="space-y-2">
            {alerts.map((a) => (
              <div
                key={a.id}
                className={`bg-zinc-900 border rounded-lg p-4 ${
                  a.read ? "border-zinc-800 opacity-60" : "border-blue-800"
                }`}
              >
                <div className="flex items-start justify-between">
                  <div>
                    <div className="flex items-center gap-2 mb-1">
                      <span className="text-xs bg-zinc-800 text-zinc-300 px-2 py-0.5 rounded">
                        {a.alert_type}
                      </span>
                      <span className="text-xs text-zinc-500">
                        {a.address} &middot;{" "}
                        {a.created_at
                          ? new Date(a.created_at).toLocaleDateString()
                          : ""}
                      </span>
                    </div>
                    <p className="text-sm">{a.message}</p>
                    {a.detail && (
                      <p className="text-xs text-zinc-400 mt-1">{a.detail}</p>
                    )}
                  </div>
                  {!a.read && (
                    <button
                      onClick={() => handleMarkRead(a.id)}
                      className="text-xs text-zinc-500 hover:text-zinc-300 px-2 py-1"
                    >
                      Mark read
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
