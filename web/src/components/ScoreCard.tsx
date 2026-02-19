interface ScoreCardProps {
  label: string;
  score: number;
  max?: number;
  color?: string;
  subtitle?: string;
}

export function ScoreCard({
  label,
  score,
  max = 100,
  color = "blue",
  subtitle,
}: ScoreCardProps) {
  const pct = Math.min(100, (score / max) * 100);

  const colors: Record<string, { bar: string; text: string }> = {
    blue: { bar: "bg-blue-500", text: "text-blue-400" },
    green: { bar: "bg-emerald-500", text: "text-emerald-400" },
    amber: { bar: "bg-amber-500", text: "text-amber-400" },
    red: { bar: "bg-red-500", text: "text-red-400" },
  };
  const c = colors[color] || colors.blue;

  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4">
      <div className="flex items-baseline justify-between mb-2">
        <span className="text-xs text-zinc-400 uppercase tracking-wider">
          {label}
        </span>
        <span className={`text-2xl font-bold ${c.text}`}>{score}</span>
      </div>
      <div className="w-full bg-zinc-800 rounded-full h-1.5">
        <div
          className={`${c.bar} h-1.5 rounded-full transition-all`}
          style={{ width: `${pct}%` }}
        />
      </div>
      {subtitle && (
        <p className="text-xs text-zinc-500 mt-2">{subtitle}</p>
      )}
    </div>
  );
}
