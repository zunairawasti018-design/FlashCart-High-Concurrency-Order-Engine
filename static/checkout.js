const form = document.querySelector("#checkout-form");
const button = document.querySelector("#submit-button");
const message = document.querySelector("#message");
const logConsole = document.querySelector("#log-console");
const inventoryList = document.querySelector("#inventory-list");
const priceSummary = document.querySelector("#price-summary");
const consoleConnection = document.querySelector("#console-connection");
const consoleStatus = consoleConnection.querySelector(".live-label");
let idempotencyKey = crypto.randomUUID();
let statusStream;
let inventoryItems = [];

function formatCents(cents, currency) {
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
  }).format(cents / 100);
}

function updatePriceSummary() {
  const selected = inventoryItems.find((item) => item.sku === form.elements.sku.value);
  if (!selected) {
    priceSummary.textContent = "Price is currently unavailable.";
    return;
  }
  const quantity = Number(form.elements.quantity.value) || 0;
  const totalCents = selected.unit_price_cents * quantity;
  priceSummary.textContent =
    `Unit price: ${formatCents(selected.unit_price_cents, selected.currency)} · ` +
    `Total: ${formatCents(totalCents, selected.currency)}`;
}

function addLog(entry) {
  const empty = logConsole.querySelector(".console-empty");
  empty?.remove();

  const row = document.createElement("li");
  const time = document.createElement("time");
  time.dateTime = entry.created_at;
  time.textContent = new Date(entry.created_at).toLocaleTimeString();

  const level = document.createElement("span");
  level.className = `console-level ${entry.level.toLowerCase()}`;
  level.textContent = entry.level;

  const event = document.createElement("span");
  event.textContent = entry.event_type;

  const detail = document.createElement("span");
  detail.className = "console-message";
  detail.textContent = entry.checkout_id
    ? `${entry.message} · ${entry.checkout_id.slice(0, 8)}`
    : entry.message;

  row.append(time, level, event, detail);
  logConsole.append(row);
  while (logConsole.children.length > 100) {
    logConsole.firstElementChild.remove();
  }
  logConsole.scrollTop = logConsole.scrollHeight;
}

async function loadInventory() {
  try {
    const response = await fetch("/api/inventory");
    if (!response.ok) throw new Error("Inventory is unavailable");
    const { items } = await response.json();
    inventoryItems = items;
    const selectedSku = form.elements.sku.value;
    const availableItems = items.filter((item) => item.available_quantity > 0);
    form.elements.sku.replaceChildren(...availableItems.map((item) => {
      const option = document.createElement("option");
      option.value = item.sku;
      option.textContent =
        `${item.sku} · ${formatCents(item.unit_price_cents, item.currency)}`;
      return option;
    }));
    if (availableItems.some((item) => item.sku === selectedSku)) {
      form.elements.sku.value = selectedSku;
    }
    inventoryList.replaceChildren();
    const dot = document.createElement("span");
    dot.className = "stock-dot";
    inventoryList.append(dot);
    inventoryList.append(
      availableItems.length
        ? availableItems.map((item) => `${item.sku}: ${item.available_quantity} left`).join(" · ")
        : "All items are currently sold out",
    );
    form.elements.sku.disabled = availableItems.length === 0;
    button.disabled = availableItems.length === 0;
    updatePriceSummary();
  } catch {
    inventoryList.replaceChildren("Inventory status unavailable");
  }
}

const operationsStream = new EventSource("/api/operations/events");
operationsStream.addEventListener("open", () => {
  consoleStatus.textContent = "LIVE";
  consoleConnection.classList.remove("offline", "reconnecting");
});
operationsStream.addEventListener("log", (event) => {
  addLog(JSON.parse(event.data));
});
operationsStream.addEventListener("error", () => {
  const state = operationsStream.readyState === EventSource.CLOSED
    ? "OFFLINE"
    : "RECONNECTING";
  consoleStatus.textContent = state;
  consoleConnection.classList.toggle("offline", state === "OFFLINE");
  consoleConnection.classList.toggle("reconnecting", state === "RECONNECTING");
  if (operationsStream.readyState === EventSource.CLOSED) {
    addLog({
      created_at: new Date().toISOString(),
      level: "WARNING",
      event_type: "console.disconnected",
      message: "Live log connection disconnected",
    });
  }
});

loadInventory();
form.elements.sku.addEventListener("change", updatePriceSummary);
form.elements.quantity.addEventListener("input", updatePriceSummary);

form.addEventListener("input", () => {
  statusStream?.close();
  statusStream = undefined;
  idempotencyKey = crypto.randomUUID();
  message.textContent = "";
  message.classList.remove("error");
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!form.reportValidity()) return;

  button.disabled = true;
  message.textContent = "Sending your request…";
  message.classList.remove("error");

  const payload = {
    email: form.elements.email.value,
    items: [{
      sku: form.elements.sku.value.trim(),
      quantity: Number(form.elements.quantity.value),
    }],
  };

  try {
    const response = await fetch("/api/checkout", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.detail || "Unable to submit checkout.");
    }
    message.textContent =
      `Checkout ${result.status} (${result.checkout_id}) · ` +
      `${formatCents(result.total_cents, result.currency)}. Waiting for live updates…`;
    addLog({
      created_at: new Date().toISOString(),
      level: "INFO",
      event_type: "checkout.submitted",
      checkout_id: result.checkout_id,
      message: "Checkout request accepted",
    });
    statusStream?.close();
    statusStream = new EventSource(`/api/checkouts/${encodeURIComponent(result.checkout_id)}/events`);
    statusStream.addEventListener("status", (event) => {
      const update = JSON.parse(event.data);
      const failureMessage = update.failure_reason === "insufficient_stock"
        ? "Sold out."
        : update.failure_reason === "processing_error"
          ? "Checkout could not be processed."
          : `Checkout ${update.status} (${update.checkout_id}).`;
      message.textContent =
        `${failureMessage} Total: ${formatCents(update.total_cents, update.currency)}.`;
      addLog({
        created_at: new Date().toISOString(),
        level: update.status === "failed" ? "WARNING" : "INFO",
        event_type: `checkout.${update.status}`,
        checkout_id: update.checkout_id,
        message: `Checkout ${update.status}`,
      });
      if (update.status === "completed" || update.status === "failed") {
        statusStream.close();
        if (update.status === "failed") message.classList.add("error");
        loadInventory();
      }
    });
    statusStream.addEventListener("error", () => {
      if (statusStream.readyState === EventSource.CLOSED) {
        message.textContent = "Live updates disconnected. You can refresh to check the status.";
        message.classList.add("error");
      }
    });
  } catch (error) {
    message.textContent = error instanceof Error
      ? error.message
      : "Network error. Please try again.";
    message.classList.add("error");
  } finally {
    button.disabled = false;
  }
});
