/**
 * The prototype's one working example: a self-contained generated chart used
 * by the generative-UI tests and as a reference for the ```genui fenced-block
 * contract. It is entirely declarative — inline `<style>` + HTML/CSS, no
 * `<script>` and no navigation/loading constructs — so it survives
 * {@link sanitizeGenUiHtml} intact and renders under the fully-inert sandbox
 * and the strict {@link GENUI_CSP}, making zero network requests.
 */
export const SAMPLE_GENUI_CHART = `<style>
  .card { font: 500 13px system-ui, sans-serif; color: #0f172a; }
  h3 { margin: 0 0 16px; font-size: 14px; }
  .bars { display: grid; grid-template-columns: repeat(5, 1fr); gap: 14px; align-items: end; height: 150px; }
  .bar { background: linear-gradient(180deg, #6366f1, #4338ca); border-radius: 6px 6px 0 0; position: relative; }
  .bar span { position: absolute; top: -18px; left: 0; right: 0; text-align: center; font-size: 11px; color: #475569; }
  .labels { display: grid; grid-template-columns: repeat(5, 1fr); gap: 14px; margin-top: 6px; text-align: center; font-size: 11px; color: #64748b; }
</style>
<div class="card">
  <h3>Weekly deploys</h3>
  <div class="bars">
    <div class="bar" style="height: 50%"><span>4</span></div>
    <div class="bar" style="height: 88%"><span>7</span></div>
    <div class="bar" style="height: 38%"><span>3</span></div>
    <div class="bar" style="height: 100%"><span>8</span></div>
    <div class="bar" style="height: 75%"><span>6</span></div>
  </div>
  <div class="labels">
    <div>Mon</div><div>Tue</div><div>Wed</div><div>Thu</div><div>Fri</div>
  </div>
</div>`;
