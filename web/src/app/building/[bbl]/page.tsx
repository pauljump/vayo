"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { apiFetch, apiPost } from "@/lib/api";
import {
  Building,
  BuildingScore,
  Complaint,
  ComplaintStats,
  Sale,
  Permit,
  Violation,
  Contact,
  Litigation,
  BOROUGH_MAP,
} from "@/lib/types";
import { ScoreCard } from "@/components/ScoreCard";
import { ComplaintsChart } from "@/components/ComplaintsChart";

type Tab =
  | "signals"
  | "complaints"
  | "sales"
  | "permits"
  | "violations"
  | "contacts"
  | "listings";

export default function BuildingDossier() {
  const { bbl } = useParams<{ bbl: string }>();
  const bblNum = parseInt(bbl, 10);

  const [building, setBuilding] = useState<Building | null>(null);
  const [score, setScore] = useState<BuildingScore | null>(null);
  const [tab, setTab] = useState<Tab>("signals");
  const [loading, setLoading] = useState(true);

  // Tab data
  const [complaints, setComplaints] = useState<Complaint[]>([]);
  const [complaintStats, setComplaintStats] = useState<ComplaintStats | null>(
    null
  );
  const [sales, setSales] = useState<Sale[]>([]);
  const [permits, setPermits] = useState<Permit[]>([]);
  const [violations, setViolations] = useState<Violation[]>([]);
  const [contacts, setContacts] = useState<Contact[]>([]);
  const [litigation, setLitigation] = useState<Litigation[]>([]);
  const [listings, setListings] = useState<{
    elliman: unknown[];
    streeteasy: { building: unknown; units: unknown[] };
  } | null>(null);

  // Which tabs have been loaded
  const [loaded, setLoaded] = useState<Set<Tab>>(new Set());

  useEffect(() => {
    if (!bblNum) return;
    setLoading(true);
    Promise.all([
      apiFetch<Building>(`/api/buildings/${bblNum}`),
      apiFetch<BuildingScore>(`/api/buildings/${bblNum}/score`),
      apiFetch<ComplaintStats>(`/api/buildings/${bblNum}/complaint-stats`),
    ]).then(([b, s, cs]) => {
      setBuilding(b);
      setScore("gem_score" in s ? s : null);
      setComplaintStats(cs);
      setLoading(false);
    });
  }, [bblNum]);

  useEffect(() => {
    if (!bblNum || loaded.has(tab)) return;
    setLoaded((prev) => new Set(prev).add(tab));

    switch (tab) {
      case "complaints":
        apiFetch<Complaint[]>(`/api/buildings/${bblNum}/complaints`).then(
          setComplaints
        );
        break;
      case "sales":
        apiFetch<Sale[]>(`/api/buildings/${bblNum}/sales`).then(setSales);
        break;
      case "permits":
        apiFetch<Permit[]>(`/api/buildings/${bblNum}/permits`).then(setPermits);
        break;
      case "violations":
        apiFetch<Violation[]>(`/api/buildings/${bblNum}/violations`).then(
          setViolations
        );
        break;
      case "contacts":
        apiFetch<Contact[]>(`/api/buildings/${bblNum}/contacts`).then(
          setContacts
        );
        break;
      case "listings":
        apiFetch(`/api/buildings/${bblNum}/listings`).then((d) =>
          setListings(
            d as {
              elliman: unknown[];
              streeteasy: { building: unknown; units: unknown[] };
            }
          )
        );
        break;
    }
  }, [bblNum, tab, loaded]);

  async function handleWatch() {
    await apiPost("/api/watchlist", { bbl: bblNum });
    alert("Added to watchlist");
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[50vh]">
        <p className="text-zinc-400">Loading...</p>
      </div>
    );
  }

  if (!building) {
    return (
      <div className="max-w-4xl mx-auto px-4 py-12">
        <p className="text-zinc-400">Building not found</p>
        <Link href="/" className="text-blue-400 text-sm mt-2 block">
          Back to search
        </Link>
      </div>
    );
  }

  const signals: [string, string][] = score?.signals
    ? JSON.parse(score.signals)
    : [];

  const tabs: { key: Tab; label: string }[] = [
    { key: "signals", label: "Signals" },
    { key: "complaints", label: `Complaints (${complaintStats?.total ?? "?"})` },
    { key: "sales", label: "Sales" },
    { key: "permits", label: "Permits" },
    { key: "violations", label: "Violations" },
    { key: "contacts", label: "Contacts" },
    { key: "listings", label: "Listings" },
  ];

  return (
    <div className="max-w-5xl mx-auto px-4 py-8">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-2xl font-bold">{building.address}</h1>
            <p className="text-zinc-400 text-sm mt-1">
              {BOROUGH_MAP[building.borough || ""] || building.borough},{" "}
              {building.zipcode} &middot; BBL {building.bbl}
            </p>
          </div>
          <button
            onClick={handleWatch}
            className="bg-zinc-800 hover:bg-zinc-700 border border-zinc-700 text-sm px-4 py-2
                       rounded-lg transition"
          >
            + Watch
          </button>
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mt-6 text-sm">
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-3">
            <span className="text-zinc-500 text-xs">Built</span>
            <p className="font-medium">{building.year_built || "?"}</p>
          </div>
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-3">
            <span className="text-zinc-500 text-xs">Floors</span>
            <p className="font-medium">{building.num_floors || "?"}</p>
          </div>
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-3">
            <span className="text-zinc-500 text-xs">Units</span>
            <p className="font-medium">{building.units_residential}</p>
          </div>
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-3">
            <span className="text-zinc-500 text-xs">Owner</span>
            <p className="font-medium truncate">
              {building.owner_name || "?"}
            </p>
          </div>
        </div>
      </div>

      {/* Scores */}
      {score && (
        <div className="grid grid-cols-3 gap-4 mb-8">
          <ScoreCard
            label="Gem Score"
            score={score.gem_score}
            color={score.gem_score >= 70 ? "green" : score.gem_score >= 40 ? "blue" : "amber"}
            subtitle="Building quality"
          />
          <ScoreCard
            label="Availability"
            score={score.avail_score}
            color={
              score.avail_score >= 60
                ? "red"
                : score.avail_score >= 30
                  ? "amber"
                  : "blue"
            }
            subtitle={
              score.signal_count > 0
                ? `${score.signal_count} signals (${score.convergence})`
                : "No signals"
            }
          />
          <ScoreCard
            label="Combined"
            score={score.combined}
            max={200}
            color="blue"
            subtitle={score.rent_stabilized ? "Rent stabilized" : undefined}
          />
        </div>
      )}

      {/* Tabs */}
      <div className="border-b border-zinc-800 mb-6">
        <div className="flex gap-1 -mb-px overflow-x-auto">
          {tabs.map((t) => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 transition whitespace-nowrap ${
                tab === t.key
                  ? "border-blue-500 text-blue-400"
                  : "border-transparent text-zinc-400 hover:text-zinc-200"
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* Tab Content */}
      <div className="min-h-[400px]">
        {tab === "signals" && (
          <SignalsTab signals={signals} score={score} />
        )}
        {tab === "complaints" && (
          <ComplaintsTab
            complaints={complaints}
            stats={complaintStats}
          />
        )}
        {tab === "sales" && <SalesTab sales={sales} />}
        {tab === "permits" && <PermitsTab permits={permits} />}
        {tab === "violations" && <ViolationsTab violations={violations} />}
        {tab === "contacts" && <ContactsTab contacts={contacts} litigation={litigation} bbl={bblNum} loaded={loaded} setLitigation={setLitigation} setLoaded={setLoaded} />}
        {tab === "listings" && <ListingsTab listings={listings} />}
      </div>
    </div>
  );
}

function SignalsTab({
  signals,
  score,
}: {
  signals: [string, string][];
  score: BuildingScore | null;
}) {
  if (!score || !signals.length) {
    return (
      <p className="text-zinc-500 text-sm">
        No availability signals detected for this building.
      </p>
    );
  }
  return (
    <div className="space-y-3">
      {signals.map(([name, detail], i) => (
        <div
          key={i}
          className="bg-zinc-900 border border-zinc-800 rounded-lg p-4"
        >
          <div className="font-medium text-sm">{name}</div>
          <div className="text-zinc-400 text-xs mt-1">{detail}</div>
        </div>
      ))}
    </div>
  );
}

function ComplaintsTab({
  complaints,
  stats,
}: {
  complaints: Complaint[];
  stats: ComplaintStats | null;
}) {
  return (
    <div>
      {stats && (
        <div className="mb-6">
          <h3 className="text-sm font-medium text-zinc-300 mb-3">
            Complaints Over Time
          </h3>
          <ComplaintsChart data={stats.by_year} />
          {stats.by_category.length > 0 && (
            <div className="mt-4">
              <h4 className="text-xs text-zinc-500 mb-2 uppercase tracking-wider">
                Top Categories
              </h4>
              <div className="flex flex-wrap gap-2">
                {stats.by_category.map((c, i) => (
                  <span
                    key={i}
                    className="bg-zinc-800 text-zinc-300 text-xs px-2 py-1 rounded"
                  >
                    {c.category || "Unknown"}: {c.cnt}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
      <DataTable
        columns={["Date", "Category", "Subcategory", "Status", "Severity", "Unit"]}
        rows={complaints.map((c) => [
          c.received_date?.slice(0, 10) || "",
          c.category || "",
          c.subcategory || "",
          c.status || "",
          c.severity || "",
          c.unit || "",
        ])}
      />
    </div>
  );
}

function SalesTab({ sales }: { sales: Sale[] }) {
  return (
    <DataTable
      columns={["Date", "Type", "Amount", "Buyer", "Seller", "Unit"]}
      rows={sales.map((s) => [
        s.recorded_date?.slice(0, 10) || "",
        s.doc_type || "",
        s.amount ? `$${s.amount.toLocaleString()}` : "",
        s.buyer || "",
        s.seller || "",
        s.unit || "",
      ])}
    />
  );
}

function PermitsTab({ permits }: { permits: Permit[] }) {
  return (
    <DataTable
      columns={["Date", "Type", "Description", "Status", "Cost"]}
      rows={permits.map((p) => [
        p.action_date?.slice(0, 10) || "",
        p.job_type || "",
        (p.description || "").slice(0, 60),
        p.status || "",
        p.estimated_cost ? `$${p.estimated_cost.toLocaleString()}` : "",
      ])}
    />
  );
}

function ViolationsTab({ violations }: { violations: Violation[] }) {
  return (
    <DataTable
      columns={["Date", "Type", "Severity", "Status", "Description", "Balance"]}
      rows={violations.map((v) => [
        v.issue_date?.slice(0, 10) || "",
        v.violation_type || "",
        v.severity || "",
        v.status || "",
        (v.description || "").slice(0, 50),
        v.balance_due ? `$${v.balance_due.toLocaleString()}` : "",
      ])}
    />
  );
}

function ContactsTab({ contacts, litigation, bbl, loaded, setLitigation, setLoaded }: {
  contacts: Contact[];
  litigation: Litigation[];
  bbl: number;
  loaded: Set<Tab>;
  setLitigation: (l: Litigation[]) => void;
  setLoaded: (fn: (prev: Set<Tab>) => Set<Tab>) => void;
}) {
  useEffect(() => {
    if (!loaded.has("contacts")) return;
    // Also load litigation when contacts tab opens
    apiFetch<Litigation[]>(`/api/buildings/${bbl}/litigation`).then(setLitigation);
  }, [bbl, loaded, setLitigation]);

  return (
    <div>
      <h3 className="text-sm font-medium text-zinc-300 mb-3">Registered Contacts</h3>
      <DataTable
        columns={["Role", "Company", "Name", "Registered"]}
        rows={contacts.map((c) => [
          c.role || "",
          c.company || "",
          [c.first_name, c.last_name].filter(Boolean).join(" "),
          c.registered_date?.slice(0, 10) || "",
        ])}
      />
      {litigation.length > 0 && (
        <div className="mt-6">
          <h3 className="text-sm font-medium text-zinc-300 mb-3">HPD Litigation</h3>
          <DataTable
            columns={["Opened", "Type", "Status"]}
            rows={litigation.map((l) => [
              l.opened_date?.slice(0, 10) || "",
              l.case_type || "",
              l.status || "",
            ])}
          />
        </div>
      )}
    </div>
  );
}

function ListingsTab({
  listings,
}: {
  listings: {
    elliman: unknown[];
    streeteasy: { building: unknown; units: unknown[] };
  } | null;
}) {
  if (!listings) return <p className="text-zinc-500 text-sm">Loading...</p>;

  const elliman = listings.elliman as Array<Record<string, unknown>>;
  const seUnits = listings.streeteasy.units as Array<Record<string, unknown>>;

  return (
    <div>
      {elliman.length > 0 && (
        <div className="mb-6">
          <h3 className="text-sm font-medium text-zinc-300 mb-3">
            Elliman MLS ({elliman.length})
          </h3>
          <DataTable
            columns={["Date", "Unit", "Price", "Beds", "Bath", "SqFt", "Type", "Status"]}
            rows={elliman.map((l) => [
              (l.list_date as string)?.slice(0, 10) || "",
              (l.unit as string) || "",
              l.list_price
                ? `$${(l.list_price as number).toLocaleString()}`
                : "",
              String(l.bedrooms ?? ""),
              String(l.bathrooms_total ?? ""),
              l.living_area_sqft
                ? (l.living_area_sqft as number).toLocaleString()
                : "",
              (l.listing_type as string) || "",
              (l.listing_status as string) || "",
            ])}
          />
        </div>
      )}
      {seUnits.length > 0 && (
        <div>
          <h3 className="text-sm font-medium text-zinc-300 mb-3">
            StreetEasy ({seUnits.length})
          </h3>
          <DataTable
            columns={["Date", "Unit", "Price", "Beds", "Bath", "SqFt", "Type", "Avail"]}
            rows={seUnits.map((u) => [
              (u.date as string) || "",
              (u.unit as string) || "",
              (u.price as string) || "",
              (u.beds as string) || "",
              (u.baths as string) || "",
              (u.sqft as string) || "",
              (u.listing_type as string) || "",
              (u.availability as string) || "",
            ])}
          />
        </div>
      )}
      {elliman.length === 0 && seUnits.length === 0 && (
        <p className="text-zinc-500 text-sm">No listings found</p>
      )}
    </div>
  );
}

// Simple reusable data table
function DataTable({
  columns,
  rows,
}: {
  columns: string[];
  rows: string[][];
}) {
  if (!rows.length) {
    return <p className="text-zinc-500 text-sm">No data</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-zinc-800">
            {columns.map((c) => (
              <th
                key={c}
                className="text-left py-2 px-2 text-zinc-500 font-medium uppercase tracking-wider"
              >
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              className="border-b border-zinc-800/50 hover:bg-zinc-900/50"
            >
              {row.map((cell, j) => (
                <td key={j} className="py-2 px-2 text-zinc-300 max-w-[200px] truncate">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
