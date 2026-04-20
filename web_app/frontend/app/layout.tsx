import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "HarnessMate",
  description: "Search Micro-D parts and inspect grouped compatible mates.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
