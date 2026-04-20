import Link from "next/link";
import { notFound } from "next/navigation";

import { MateResults } from "@/components/MateResults";
import { PartOverview } from "@/components/PartOverview";
import { getGroupedMates, getPart } from "@/lib/api";

type PartPageProps = {
  params: Promise<{ id: string }>;
};

export default async function PartPage({ params }: PartPageProps) {
  const { id } = await params;

  try {
    const [part, mates] = await Promise.all([getPart(id), getGroupedMates(id)]);

    return (
      <main>
        <div className="shell stack">
          <Link className="back-link" href="/">
            ← Back to search
          </Link>
          <PartOverview part={part} />
          <MateResults mates={mates} />
        </div>
      </main>
    );
  } catch {
    notFound();
  }
}
