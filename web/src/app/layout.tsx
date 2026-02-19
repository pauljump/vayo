import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import Link from "next/link";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Vayo",
  description: "NYC Real Estate Intelligence",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased bg-zinc-950 text-zinc-100`}
      >
        <nav className="border-b border-zinc-800 bg-zinc-950/80 backdrop-blur sticky top-0 z-50">
          <div className="max-w-screen-2xl mx-auto px-4 h-14 flex items-center gap-8">
            <Link href="/" className="font-bold text-lg tracking-tight">
              Vayo
            </Link>
            <div className="flex gap-6 text-sm">
              <Link
                href="/explorer"
                className="text-zinc-400 hover:text-zinc-100 transition"
              >
                Explorer
              </Link>
              <Link
                href="/watchlist"
                className="text-zinc-400 hover:text-zinc-100 transition"
              >
                Watchlist
              </Link>
            </div>
          </div>
        </nav>
        <main>{children}</main>
      </body>
    </html>
  );
}
