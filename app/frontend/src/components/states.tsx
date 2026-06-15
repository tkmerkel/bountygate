export function Loading() {
  return <div className="kicker py-8 text-center">LOADING…</div>;
}

export function ErrorState({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="bevel-in my-4 p-4 text-center" data-testid="error-state">
      <span className="ledger-neg">FAILED TO LOAD</span>
      <span className="px-2 text-rule-gray">{message}</span>
      <button className="pixel bevel-out cursor-pointer bg-crisp px-3" onClick={onRetry}>
        RETRY
      </button>
    </div>
  );
}

export function Empty({ note }: { note: string }) {
  return <div className="kicker py-8 text-center">{note}</div>;
}

// Shown above a table when its freshest row is older than expected — surfaces an
// ingestion outage instead of letting aging prices read as live.
export function StaleBanner({ ageLabel }: { ageLabel: string }) {
  return (
    <div className="bevel-in my-3 px-3 py-2 text-center" data-testid="stale-banner">
      <span className="ledger-neg">⚠ DATA MAY BE STALE</span>
      <span className="px-2 text-rule-gray">
        latest update {ageLabel} ago · feed refreshes every 15 min
      </span>
    </div>
  );
}
