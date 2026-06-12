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
