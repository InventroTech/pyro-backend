/**
 * Pyro.ai dispatch prototype — driven by Supabase sync export (window.PYRO_SYNC_DATA).
 */
(function () {
  const SOURCE = window.PYRO_SYNC_DATA;
  if (!SOURCE?.records?.length) {
    console.error("PYRO_SYNC_DATA missing — load prototype/data/dispatch-sync-records.js first");
    return;
  }

  const RECORDS = SOURCE.records.filter((r) => !r.is_deleted && r.entity_type === "dispatch_request");
  let selectedYear = "2026";
  let currentRecord = null;
  let currentScreen = "login";

  const toastEl = document.getElementById("toast");
  const drawer = document.getElementById("drawer");
  const drawerOverlay = document.getElementById("drawer-overlay");
  let toastTimer;

  function padSr(sr) {
    const s = String(sr ?? "").trim();
    if (!s) return "—";
    return s.length >= 3 ? s : s.padStart(3, "0");
  }

  function formatDate(value) {
    if (!value) return "—";
    const d = new Date(value.includes("T") ? value : value + "T00:00:00");
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "2-digit" }).replace(/ /g, "-");
  }

  function formatDateTime(value) {
    if (!value) return "—";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleString("en-IN", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function formatInr(amount) {
    if (amount == null || amount === "") return "—";
    const n = Number(amount);
    if (Number.isNaN(n)) return String(amount);
    return "₹ " + n.toLocaleString("en-IN") + "/-";
  }

  function yearFromRecord(rec) {
    const d = rec.data?.dc_date || rec.data?.po_date || rec.data?.synced_at || rec.updated_at;
    if (!d) return null;
    const y = new Date(d.includes("T") ? d : d + "T00:00:00").getFullYear();
    return Number.isNaN(y) ? null : String(y);
  }

  function recordToListItem(rec) {
    const d = rec.data || {};
    return {
      id: rec.id,
      rec,
      sr: padSr(d.sr_no),
      company: (d.account_name || "—").toUpperCase(),
      account: d.account_name || "—",
      poLabel: "Po # :",
      po: d.po_number || "—",
      ref: d.sales_order_number || d.dc_number || d.source_row_id || "—",
    };
  }

  function getListItems(year) {
    return RECORDS.filter((r) => yearFromRecord(r) === year).map(recordToListItem);
  }

  function getAvailableYears() {
    const years = new Set(RECORDS.map(yearFromRecord).filter(Boolean));
    if (!years.size) years.add("2026");
    return [...years].sort((a, b) => Number(b) - Number(a));
  }

  function latestSyncRecord() {
    return [...RECORDS].sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at))[0] || null;
  }

  function showToast(msg) {
    toastEl.textContent = msg;
    toastEl.classList.add("show");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => toastEl.classList.remove("show"), 2400);
  }

  function setDrawer(open) {
    drawer.classList.toggle("open", open);
    drawerOverlay.classList.toggle("open", open);
  }

  function esc(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function field(lbl, val, opts = {}) {
    const cls = opts.full ? "data-field full" : "data-field";
    const valCls = opts.green ? "val text-green" : "val";
    return `<div class="${cls}"><span class="lbl">${esc(lbl)}</span><div class="${valCls}">${esc(val ?? "—")}</div></div>`;
  }

  function boolPill(label, on) {
    if (!on) return "";
    return `<span class="pill">${esc(label)}</span>`;
  }

  function section(title, icon, bgClass, bodyHtml, sub) {
    const subHtml = sub ? `<span class="sub">${esc(sub)}</span>` : "";
    return `
      <div class="section ${bgClass}">
        <div class="section-header">${icon} ${esc(title)} ${subHtml}</div>
        ${bodyHtml}
      </div>`;
  }

  function renderDetail(rec) {
    const d = rec.data || {};
    const pills = [
      boolPill("DC Recd", d.dc_received_in_office),
      boolPill("DC in Office", d.dc_in_office),
      boolPill("L/R Recd in Office", !!d.lr_received_in_office),
    ]
      .filter(Boolean)
      .join("");

    document.getElementById("detail-hero").innerHTML = `
      <div><div class="lbl">Sr. No.</div><div class="val">${esc(padSr(d.sr_no))}</div></div>
      <div><div class="lbl">DC Date</div><div class="val">${esc(formatDate(d.dc_date))}</div></div>
      <div><div class="lbl">Account Name</div><div class="val">${esc(d.account_name)}</div></div>
      <div><div class="lbl">DC Number</div><div class="val">${esc(d.dc_number)}</div></div>`;

    const syncBanner = `
      <div class="sync-banner">
        <span class="sync-dot"></span>
        Synced ${esc(formatDateTime(d.synced_at || rec.updated_at))}
        <span class="sync-id">#${rec.id}</span>
      </div>`;

    const html = [
      syncBanner,
      section(
        "Product & Order",
        "🛒",
        "bg-purple",
        `<div class="field-grid">
          ${field("Product", d.products, { full: true })}
          <div class="field-grid cols-3">
            ${field("Terms", d.terms)}
            ${field("Qty", d.quantity)}
            ${field("Amount", formatInr(d.amount))}
          </div>
          <div class="field-grid cols-2" style="margin-top:8px">
            ${field("PO #", d.po_number)}
            ${field("PO Date", formatDate(d.po_date))}
          </div>
          ${field("Engineer", d.engineer, { full: true })}
          ${field("Sales Order #", d.sales_order_number, { full: true })}
        </div>`
      ),
      section(
        "Consignee Details",
        "📍",
        "bg-green",
        `<div class="field-grid cols-2">
          ${field("City", d.consignee_city)}
          ${field("Serial #", d.serial_numbers)}
        </div>`
      ),
      section(
        "Dispatch Tracking",
        "📅",
        "bg-yellow",
        `<div class="field-grid">
          ${field("Scanned DC Sent", formatDate(d.date_scanned_copy_dc_to_office), { full: true })}
          <div class="field-grid cols-2" style="margin-top:8px">
            ${field("Material Dispatch", formatDate(d.date_of_material_dispatch))}
            ${field("Godown to Office", formatDate(d.date_dispatch_godown_dc_to_office))}
          </div>
          ${field("Remarks", d.remarks || "—", { full: true })}
          <div class="status-pills">${pills || '<span class="pill-muted">No status flags</span>'}</div>
        </div>`,
        "Umesh / Akash"
      ),
      section(
        "Transport & Logistics",
        "🚚",
        "bg-orange",
        `<div class="field-grid cols-2">
          ${field("E-Way Bill #", d.e_way_bill_number)}
          ${field("E-Way Server Update", d.e_way_updated_in_server === "TRUE" || d.e_way_updated_in_server === true ? "Yes" : "No", { green: true })}
          ${field("Transporter", d.transporter_name)}
          ${field("Vehicle #", d.vehicle_number)}
          ${field("Freight Mode", d.freight_mode)}
          ${field("Freight Amount", formatInr(d.freight_amount))}
          ${field("L/R #", d.lr_number, { full: true })}
          ${field("L/R Date", formatDate(d.lr_date))}
          ${field("Dispatch L/R to Office", formatDate(d.date_lr_dispatch_to_office), { full: true })}
          ${field("Delivery at Consignee", formatDate(d.date_delivery_at_consignee), { full: true })}
        </div>`,
        "Arvind G"
      ),
      section(
        "Warranty & Documentation",
        "🛡",
        "bg-pink",
        `<div class="field-grid cols-2">
          ${field("E-Warranty #", d.e_warranty_number)}
          ${field("E-Warranty Update", formatDate(d.e_warranty_updated_date), { green: true })}
          ${field("Email (Invoice)", formatDate(d.date_email_inv_details))}
        </div>`,
        "Akash"
      ),
      section(
        "Customer Communication",
        "✉️",
        "bg-green",
        `<div class="field-grid">
          ${field("Email (Invoice)", formatDate(d.date_email_inv_details))}
          ${field("Email (TC Details)", formatDate(d.date_email_tc_details))}
          ${field("Courier Sent", formatDate(d.date_courier_to_customer))}
          ${field("Email (Vehicle Details)", formatDate(d.date_email_vehicle_dispatch_details), { full: true })}
        </div>`,
        "Tulsi"
      ),
      section(
        "SIS CTF Details",
        "📄",
        "bg-blue",
        `<div class="field-grid">
          ${field("SIS CTF Pump Model", d.sis_ctf_pump_model)}
          ${field("Model Serial #", d.sis_ctf_model_serial_number, { full: true })}
          ${field("SIS CTF CRM #", d.sis_ctf_crm_number)}
          ${field("SIS CTF Date", formatDate(d.sis_ctf_date))}
          ${field("SIS CTF Mail", d.sis_ctf_mail ? "Sent" : "—", { green: !!d.sis_ctf_mail })}
          ${field("SIS CTF Done", d.sis_ctf_done, { green: d.sis_ctf_done === "DONE" })}
        </div>`,
        "Umesh"
      ),
      section(
        "Final Checks",
        "✅",
        "bg-tan",
        `<div class="field-grid cols-2">
          ${field("Checked/Gathered", formatDate(d.checked_gather) + (d.checked_gather ? " ✓" : ""))}
          ${field("Barcode", formatDate(d.barcode))}
          ${field("In Time", d.godown_in_time)}
          ${field("Out Time", d.godown_out_time)}
        </div>`,
        "Darshan S"
      ),
    ].join("");

    document.getElementById("detail-sections").innerHTML = html;
    document.getElementById("detail-scroll").scrollTop = 0;
  }

  function renderDispatchCards(filter = "") {
    const container = document.getElementById("dispatch-cards");
    const q = filter.trim().toLowerCase();
    const items = getListItems(selectedYear);

    document.getElementById("list-year").textContent = selectedYear;
    const rangeEl = document.querySelector(".period-label .range");
    if (rangeEl) {
      const months = items
        .map((i) => {
          const raw = i.rec.data?.dc_date || i.rec.data?.po_date;
          if (!raw) return null;
          return new Date(raw + (raw.includes("T") ? "" : "T00:00:00")).toLocaleString("en", { month: "short" }).toUpperCase();
        })
        .filter(Boolean);
      rangeEl.textContent = months.length ? [...new Set(months)].join(" · ") : "SYNCED DATA";
    }

    container.innerHTML = "";
    if (!items.length) {
      container.innerHTML = `<p class="empty-state">No dispatch records for ${esc(selectedYear)} in sync export.</p>`;
      return;
    }

    items
      .filter((item) => {
        if (!q) return true;
        const hay = [item.sr, item.company, item.po, item.ref, item.account].join(" ").toLowerCase();
        return hay.includes(q);
      })
      .forEach((item) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "dispatch-card";
        btn.innerHTML = `
          <div class="top">
            <span class="sr">${esc(item.sr)}</span>
            <span class="po">${esc(item.poLabel)}${esc(item.po)}</span>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:flex-end;gap:8px">
            <span class="company">${esc(item.company)}</span>
            <span class="ref">${esc(item.ref)}</span>
          </div>`;
        btn.addEventListener("click", () => {
          currentRecord = item.rec;
          renderDetail(item.rec);
          goTo("dispatch-detail");
        });
        container.appendChild(btn);
      });
  }

  function renderYearGrid() {
    const grid = document.getElementById("year-grid");
    if (!grid) return;
    const years = getAvailableYears();
    grid.innerHTML = "";
    years.forEach((year) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "year-card";
      btn.textContent = year;
      const count = getListItems(year).length;
      btn.dataset.year = year;
      if (count) {
        btn.dataset.goto = "dispatch-list";
        btn.title = `${count} record(s)`;
      } else {
        btn.dataset.action = "toast";
        btn.dataset.msg = `No synced records for ${year}`;
      }
      grid.appendChild(btn);
    });
    // Placeholder years from design (disabled)
    ["2025", "2024", "2023", "2022", "2021", "2020", "2019"].forEach((year) => {
      if (years.includes(year)) return;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "year-card year-card--muted";
      btn.textContent = year;
      btn.dataset.action = "toast";
      btn.dataset.msg = `No synced data for ${year}`;
      grid.appendChild(btn);
    });
  }

  function renderSyncStatus() {
    const latest = latestSyncRecord();
    const el = document.getElementById("sync-status-body");
    if (!el) return;

    if (!latest) {
      el.innerHTML = `<p class="empty-state">No sync records loaded.</p>`;
      return;
    }

    const d = latest.data || {};
    el.innerHTML = `
      <div class="sync-status-card sync-status-card--ok">
        <div class="sync-status-badge">Synced</div>
        <h3 class="sync-status-title">${esc(d.account_name || "Dispatch request")}</h3>
        <p class="sync-status-meta">Entity: <strong>${esc(latest.entity_type)}</strong></p>
      </div>
      <div class="sync-kv-grid">
        <div class="sync-kv"><span class="k">Job ID</span><span class="v">#${latest.id}</span></div>
        <div class="sync-kv"><span class="k">Sr. No.</span><span class="v">${esc(padSr(d.sr_no))}</span></div>
        <div class="sync-kv"><span class="k">DC Number</span><span class="v">${esc(d.dc_number)}</span></div>
        <div class="sync-kv"><span class="k">Created</span><span class="v">${esc(formatDateTime(latest.created_at))}</span></div>
        <div class="sync-kv"><span class="k">Updated</span><span class="v">${esc(formatDateTime(latest.updated_at))}</span></div>
        <div class="sync-kv"><span class="k">Synced at</span><span class="v">${esc(formatDateTime(d.synced_at))}</span></div>
        <div class="sync-kv full"><span class="k">Tenant</span><span class="v mono">${esc(latest.tenant_id)}</span></div>
        <div class="sync-kv full"><span class="k">Source row</span><span class="v">${esc(d.source_row_id)}</span></div>
      </div>
      <button type="button" class="cta-btn" data-goto="dispatch-detail" data-record-id="${latest.id}">View dispatch detail</button>
      <button type="button" class="btn btn-ghost" style="width:100%;margin-top:10px" data-goto="dispatch-list" data-year="${yearFromRecord(latest) || "2026"}">Open dispatch list</button>
    `;
  }

  function renderDashboardSync() {
    const latest = latestSyncRecord();
    const card = document.getElementById("dashboard-sync-card");
    if (!card || !latest) return;
    const d = latest.data || {};
    card.innerHTML = `
      <div class="sync-mini-header">
        <span class="sync-dot"></span> Latest Supabase sync
      </div>
      <div class="sync-mini-title">${esc(d.account_name)}</div>
      <div class="sync-mini-meta">Sr ${esc(padSr(d.sr_no))} · ${esc(formatDateTime(d.synced_at || latest.updated_at))}</div>
      <button type="button" class="sync-mini-link" data-goto="sync-status">View sync status →</button>
    `;
    card.hidden = false;
  }

  function goTo(screenId, opts = {}) {
    if (opts.year) selectedYear = opts.year;
    if (opts.recordId) {
      const rec = RECORDS.find((r) => r.id === Number(opts.recordId));
      if (rec) {
        currentRecord = rec;
        renderDetail(rec);
      }
    }

    currentScreen = screenId;
    document.querySelectorAll(".screen").forEach((el) => {
      el.classList.toggle("active", el.dataset.screen === screenId);
    });
    document.querySelectorAll(".proto-nav button").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.goto === screenId);
    });
    setDrawer(false);

    if (screenId === "dispatch-list") renderDispatchCards();
    if (screenId === "sync-status") renderSyncStatus();
    if (screenId === "dispatch-detail" && currentRecord) renderDetail(currentRecord);
  }

  document.addEventListener("click", (e) => {
    const t = e.target.closest("[data-goto],[data-action],[data-drawer],[data-year]");
    if (!t) return;

    if (t.dataset.drawer === "open") {
      setDrawer(true);
      return;
    }
    if (t.dataset.drawer === "close") {
      setDrawer(false);
      if (!t.dataset.goto && !t.dataset.action) return;
    }

    if (t.dataset.year) selectedYear = t.dataset.year;

    if (t.dataset.goto) {
      goTo(t.dataset.goto, {
        year: t.dataset.year,
        recordId: t.dataset.recordId,
      });
      return;
    }

    if (t.dataset.action === "toast" && t.dataset.msg) showToast(t.dataset.msg);
  });

  document.getElementById("dispatch-search")?.addEventListener("input", (e) => {
    renderDispatchCards(e.target.value);
  });

  // Init
  renderYearGrid();
  renderDashboardSync();
  currentRecord = RECORDS[0] || null;
  if (currentRecord) {
    selectedYear = yearFromRecord(currentRecord) || "2026";
    renderDetail(currentRecord);
  }
  renderDispatchCards();
  renderSyncStatus();
})();
