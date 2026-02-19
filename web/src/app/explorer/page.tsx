"use client";

import { useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import { apiFetch } from "@/lib/api";
import { BOROUGH_MAP } from "@/lib/types";

interface BuildingRow {
  bbl: number;
  address: string | null;
  zipcode: string | null;
  borough: string | null;
  year_built: number | null;
  num_floors: number | null;
  units_residential: number | null;
  owner_name: string | null;
  avg_unit_sqft: number | null;
  gem_score?: number;
  avail_score?: number;
  combined?: number;
  signal_count?: number;
  convergence?: string | null;
  rent_stabilized?: boolean | number;
  stab_units?: number;
}

interface FilterState {
  borough: string;
  zip: string;
  minUnits: number;
  minGem: string;
  rentStabilized: boolean;
  sort: string;
  dir: string;
}

export default function ExplorerPage() {
  const router = useRouter();
  const [filters, setFilters] = useState<FilterState>({
    borough: "",
    zip: "",
    minUnits: 4,
    minGem: "",
    rentStabilized: false,
    sort: "combined",
    dir: "desc",
  });
  const [buildings, setBuildings] = useState<BuildingRow[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(0);
  const limit = 100;

  const fetchBuildings = useCallback(async () => {
    setLoading(true);
    const params = new URLSearchParams();
    if (filters.borough) params.set("borough", filters.borough);
    if (filters.zip) params.set("zip", filters.zip);
    params.set("min_units", String(filters.minUnits));
    if (filters.minGem) params.set("min_gem", filters.minGem);
    if (filters.rentStabilized) params.set("rent_stabilized", "true");
    params.set("sort", filters.sort);
    params.set("dir", filters.dir);
    params.set("limit", String(limit));
    params.set("offset", String(page * limit));

    try {
      const data = await apiFetch<{ total: number; buildings: BuildingRow[] }>(
        `/api/buildings?${params}`
      );
      setBuildings(data.buildings);
      setTotal(data.total);
    } catch {
      setBuildings([]);
    } finally {
      setLoading(false);
    }
  }, [filters, page]);

  useEffect(() => {
    fetchBuildings();
  }, [fetchBuildings]);

  function updateFilter(key: keyof FilterState, value: string | number | boolean) {
    setPage(0);
    setFilters((prev) => ({ ...prev, [key]: value }));
  }

  const sortOptions = [
    { value: "combined", label: "Combined Score" },
    { value: "gem_score", label: "Gem Score" },
    { value: "avail_score", label: "Availability" },
    { value: "units_residential", label: "Units" },
    { value: "year_built", label: "Year Built" },
  ];

  return (
    <div className="max-w-screen-2xl mx-auto px-4 py-6">
      <h1 className="text-xl font-bold mb-4">Market Explorer</h1>

      {/* Filter Bar */}
      <div className="flex flex-wrap gap-3 mb-6 items-end">
        <div>
          <label className="text-xs text-zinc-500 block mb-1">Borough</label>
          <select
            value={filters.borough}
            onChange={(e) => updateFilter("borough", e.target.value)}
            className="bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm"
          >
            <option value="">All</option>
            {Object.entries(BOROUGH_MAP).map(([k, v]) => (
              <option key={k} value={k}>
                {v}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="text-xs text-zinc-500 block mb-1">ZIP</label>
          <input
            type="text"
            value={filters.zip}
            onChange={(e) => updateFilter("zip", e.target.value)}
            placeholder="e.g. 10003"
            className="bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm w-24"
          />
        </div>

        <div>
          <label className="text-xs text-zinc-500 block mb-1">Min Units</label>
          <input
            type="number"
            value={filters.minUnits}
            onChange={(e) =>
              updateFilter("minUnits", parseInt(e.target.value) || 1)
            }
            className="bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm w-20"
          />
        </div>

        <div>
          <label className="text-xs text-zinc-500 block mb-1">Min Gem</label>
          <input
            type="number"
            value={filters.minGem}
            onChange={(e) => updateFilter("minGem", e.target.value)}
            placeholder="0"
            className="bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm w-20"
          />
        </div>

        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            id="rs"
            checked={filters.rentStabilized}
            onChange={(e) => updateFilter("rentStabilized", e.target.checked)}
            className="rounded"
          />
          <label htmlFor="rs" className="text-sm text-zinc-400">
            Rent Stabilized
          </label>
        </div>

        <div>
          <label className="text-xs text-zinc-500 block mb-1">Sort</label>
          <select
            value={filters.sort}
            onChange={(e) => updateFilter("sort", e.target.value)}
            className="bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm"
          >
            {sortOptions.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <button
          onClick={() =>
            updateFilter("dir", filters.dir === "desc" ? "asc" : "desc")
          }
          className="bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm hover:bg-zinc-800"
        >
          {filters.dir === "desc" ? "DESC" : "ASC"}
        </button>
      </div>

      {/* Results count */}
      <div className="flex items-center justify-between mb-3">
        <p className="text-sm text-zinc-400">
          {loading ? "Loading..." : `${total.toLocaleString()} buildings`}
        </p>
        <div className="flex gap-2">
          <button
            onClick={() => setPage((p) => Math.max(0, p - 1))}
            disabled={page === 0}
            className="text-xs px-3 py-1 bg-zinc-900 border border-zinc-700 rounded disabled:opacity-30"
          >
            Prev
          </button>
          <span className="text-xs text-zinc-500 py-1">
            Page {page + 1}
          </span>
          <button
            onClick={() => setPage((p) => p + 1)}
            disabled={buildings.length < limit}
            className="text-xs px-3 py-1 bg-zinc-900 border border-zinc-700 rounded disabled:opacity-30"
          >
            Next
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-zinc-800">
              <th className="text-left py-2 px-2 text-zinc-500 font-medium">
                Address
              </th>
              <th className="text-left py-2 px-2 text-zinc-500 font-medium">
                ZIP
              </th>
              <th className="text-left py-2 px-2 text-zinc-500 font-medium">
                Borough
              </th>
              <th className="text-right py-2 px-2 text-zinc-500 font-medium">
                Units
              </th>
              <th className="text-right py-2 px-2 text-zinc-500 font-medium">
                Built
              </th>
              <th className="text-right py-2 px-2 text-zinc-500 font-medium">
                SqFt
              </th>
              <th className="text-right py-2 px-2 text-zinc-500 font-medium">
                Gem
              </th>
              <th className="text-right py-2 px-2 text-zinc-500 font-medium">
                Avail
              </th>
              <th className="text-right py-2 px-2 text-zinc-500 font-medium">
                Comb
              </th>
              <th className="text-center py-2 px-2 text-zinc-500 font-medium">
                Sig
              </th>
              <th className="text-center py-2 px-2 text-zinc-500 font-medium">
                RS
              </th>
            </tr>
          </thead>
          <tbody>
            {buildings.map((b) => (
              <tr
                key={b.bbl}
                onClick={() => router.push(`/building/${b.bbl}`)}
                className="border-b border-zinc-800/50 hover:bg-zinc-900/70 cursor-pointer"
              >
                <td className="py-2 px-2 text-zinc-200 max-w-[200px] truncate">
                  {b.address}
                </td>
                <td className="py-2 px-2 text-zinc-400">{b.zipcode}</td>
                <td className="py-2 px-2 text-zinc-400">{b.borough}</td>
                <td className="py-2 px-2 text-right text-zinc-300">
                  {b.units_residential}
                </td>
                <td className="py-2 px-2 text-right text-zinc-400">
                  {b.year_built || "?"}
                </td>
                <td className="py-2 px-2 text-right text-zinc-400">
                  {b.avg_unit_sqft || "?"}
                </td>
                <td className="py-2 px-2 text-right font-medium">
                  <ScoreBadge score={b.gem_score} type="gem" />
                </td>
                <td className="py-2 px-2 text-right font-medium">
                  <ScoreBadge score={b.avail_score} type="avail" />
                </td>
                <td className="py-2 px-2 text-right font-medium text-zinc-200">
                  {b.combined ?? "-"}
                </td>
                <td className="py-2 px-2 text-center text-zinc-400">
                  {b.signal_count ?? "-"}
                </td>
                <td className="py-2 px-2 text-center">
                  {b.rent_stabilized ? (
                    <span className="text-amber-400">RS</span>
                  ) : (
                    ""
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ScoreBadge({
  score,
  type,
}: {
  score: number | undefined;
  type: "gem" | "avail";
}) {
  if (score === undefined || score === null) return <span className="text-zinc-600">-</span>;

  let color = "text-zinc-400";
  if (type === "gem") {
    if (score >= 70) color = "text-emerald-400";
    else if (score >= 50) color = "text-blue-400";
    else if (score >= 30) color = "text-zinc-300";
  } else {
    if (score >= 60) color = "text-red-400";
    else if (score >= 30) color = "text-amber-400";
    else if (score > 0) color = "text-blue-400";
  }

  return <span className={color}>{score}</span>;
}
