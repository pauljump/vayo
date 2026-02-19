"use client";

import { useState } from "react";
import { apiFetch } from "@/lib/api";
import { Building } from "@/lib/types";

interface AddressSearchProps {
  onSelect: (building: Building) => void;
  placeholder?: string;
}

export function AddressSearch({
  onSelect,
  placeholder = "Search by address...",
}: AddressSearchProps) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<Building[]>([]);
  const [open, setOpen] = useState(false);

  async function handleChange(val: string) {
    setQuery(val);
    if (val.length < 3) {
      setResults([]);
      setOpen(false);
      return;
    }
    const data = await apiFetch<{ results: Building[] }>(
      `/api/buildings/search?q=${encodeURIComponent(val)}`
    );
    setResults(data.results);
    setOpen(data.results.length > 0);
  }

  return (
    <div className="relative">
      <input
        type="text"
        value={query}
        onChange={(e) => handleChange(e.target.value)}
        onFocus={() => results.length > 0 && setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 200)}
        placeholder={placeholder}
        className="w-full bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm
                   placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-blue-500"
      />
      {open && (
        <div className="absolute top-full mt-1 w-full bg-zinc-900 border border-zinc-700
                        rounded-lg shadow-lg z-50 max-h-64 overflow-y-auto">
          {results.map((b) => (
            <button
              key={b.bbl}
              onMouseDown={() => {
                onSelect(b);
                setQuery(b.address || "");
                setOpen(false);
              }}
              className="w-full text-left px-3 py-2 hover:bg-zinc-800 text-sm
                         border-b border-zinc-800 last:border-b-0"
            >
              <span className="text-zinc-200">{b.address}</span>
              <span className="text-zinc-500 ml-2 text-xs">
                {b.borough} {b.zipcode} &middot; {b.units_residential} units
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
