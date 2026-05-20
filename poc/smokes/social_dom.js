const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert/strict");

const PROJECT_ROOT = path.resolve(__dirname, "..", "..");
const POC_JS_ROOT = path.join(PROJECT_ROOT, "poc", "assets", "js");
const nativeSetTimeout = setTimeout;
const nativeClearTimeout = clearTimeout;

let document;
let localStorage;
let serviceCalls;
let localProfileIndex;

async function main() {
  const watchdog = nativeSetTimeout(() => {
    console.error("social DOM smoke failed: watchdog timeout");
    process.exit(1);
  }, 15000);

  await click("#create-profile-button");
  assert.equal(document.querySelector("#active-profile-select").value, "vn-local-a");

  await submit("#profile-form", {
    displayName: "Alice DOM",
    bio: "Perfil criado pelo smoke DOM",
  });
  assert.equal(serviceCalls.some((call) => call.name === "background_run_once"), true);

  await click("#create-profile-button");
  assert.equal(document.querySelector("#active-profile-select").value, "vn-local-b");
  await submit("#profile-form", {
    displayName: "Bob DOM",
    bio: "Segundo perfil local do smoke DOM",
  });

  const selector = document.querySelector("#active-profile-select");
  selector.value = "vn-local-a";
  await dispatch(selector, "change");
  assert.equal(document.querySelector("#profile-form").elements.displayName.value, "Alice DOM");

  await submit("#friend-form", {
    friendVirtualNodeId: "vn-remote-b",
  });
  await submit("#friend-form", {
    friendVirtualNodeId: "vn-remote-b",
  });
  const friendList = document.querySelector("#friend-list");
  assert.equal(friendList.children.length, 1);
  await dispatch(friendList.children[0], "click");
  assert.equal(document.querySelector("#message-form").elements.toVirtualNodeId.value, "vn-remote-b");

  await submit("#post-form", {
    postText: "post criado pelo smoke DOM",
  });
  const feedList = document.querySelector("#feed-list");
  assert.equal(feedList.children.length, 1);

  await submit("#message-form", {
    toVirtualNodeId: "vn-remote-b",
    text: "DM criada pelo smoke DOM",
  });
  const messageForm = document.querySelector("#message-form");
  assert.equal(messageForm.resetCount, 1);
  assert.equal(
    serviceCalls.some((call) => (
      call.name === "send_direct_message"
      && call.remoteVirtualNodeId === "vn-remote-b"
      && call.text === "DM criada pelo smoke DOM"
    )),
    true,
  );

  nativeClearTimeout(watchdog);
  console.log("OK poc social DOM smoke passed");
}

async function click(selector) {
  await dispatch(document.querySelector(selector), "click");
}

async function submit(selector, fields) {
  const form = document.querySelector(selector);
  form.setFields(fields);
  await dispatch(form, "submit");
}

async function dispatch(element, type) {
  const listener = element.listeners.get(type);
  assert.ok(listener, `listener not registered for ${type}`);
  const event = {
    currentTarget: element,
    target: element,
    preventDefault() {},
  };
  const result = listener(event);
  event.currentTarget = null;
  await result;
}

function prepareDom() {
  for (const id of [
    "app-feedback",
    "app-loading",
    "app-loading-title",
    "app-loading-message",
    "connection-pill",
    "core-status",
    "active-profile-select",
    "create-profile-button",
    "local-vn-output",
    "profile-avatar",
    "composer-avatar",
    "profile-name",
    "profile-bio",
    "friend-count",
    "friend-summary",
    "post-count",
    "friend-list",
    "feed-list",
    "event-log",
    "profile-photo",
    "refresh-status",
    "message-text",
  ]) {
    document.register(`#${id}`, new FakeElement(id));
  }

  document.register("#profile-form", new FakeFormElement("profile-form", {
    displayName: "",
    bio: "",
  }));
  document.register("#friend-form", new FakeFormElement("friend-form", {
    friendVirtualNodeId: "",
  }));
  document.register("#message-form", new FakeFormElement("message-form", {
    toVirtualNodeId: "",
    text: "",
  }));
  document.register("#post-form", new FakeFormElement("post-form", {
    postText: "",
  }));
  document.register("#post-template", new FakeTemplateElement("post-template"));
}

function loadBrowserScript(relativePath) {
  const scriptPath = path.join(POC_JS_ROOT, relativePath);
  const source = fs.readFileSync(scriptPath, "utf-8");
  vm.runInThisContext(source, { filename: scriptPath });
}

class FakeAnonNetClient {
  connectEvents() {
    return new FakeWebSocket();
  }
}

class FakeSocialService {
  async createLocalProfileNode() {
    const suffix = ["a", "b", "c"][localProfileIndex] || String(localProfileIndex + 1);
    localProfileIndex += 1;
    serviceCalls.push({ name: "create_local_profile_node" });
    return {
      id: `vn-local-${suffix}`,
      public_key: `pk-local-${suffix}`,
      kind: "social",
    };
  }

  addFriendToProfile({ profile, friendVirtualNodeId }) {
    serviceCalls.push({ name: "add_friend", friendVirtualNodeId });
    return {
      ...profile,
      friend_virtual_node_ids: [...new Set([
        ...(profile.friend_virtual_node_ids || []),
        friendVirtualNodeId,
      ])],
      updated_at: new Date().toISOString(),
    };
  }

  async sendDirectMessageToVirtualNode({ remoteVirtualNodeId, text }) {
    serviceCalls.push({
      name: "send_direct_message",
      remoteVirtualNodeId,
      text,
    });
    await Promise.resolve();
    return {
      sessionId: "session-dom-smoke",
      reused: false,
    };
  }
}

class FakeBackgroundSyncService {
  constructor() {
    this.started = false;
  }

  start() {
    this.started = true;
    serviceCalls.push({ name: "background_start" });
  }

  async runOnce({ reason }) {
    serviceCalls.push({ name: "background_run_once", reason });
  }
}

class FakeDocument {
  constructor() {
    this.elements = new Map();
    this.body = new FakeElement("body");
  }

  addEventListener() {}

  createElement(tagName) {
    return new FakeElement(tagName);
  }

  querySelector(selector) {
    const element = this.elements.get(selector);
    if (!element) {
      throw new Error(`Fake DOM element not registered: ${selector}`);
    }
    return element;
  }

  register(selector, element) {
    this.elements.set(selector, element);
  }
}

class FakeElement {
  constructor(id) {
    this.id = id;
    this.children = [];
    this.listeners = new Map();
    this.style = {};
    this.className = "";
    this.hidden = false;
    this.value = "";
    this.innerHTML = "";
    this.textContent = "";
    this.classList = {
      add: () => {},
      remove: () => {},
      toggle: () => {},
    };
  }

  addEventListener(type, listener) {
    this.listeners.set(type, listener);
  }

  append(...children) {
    for (const child of children) {
      child.parentNode = this;
    }
    this.children.push(...children);
  }

  prepend(child) {
    child.parentNode = this;
    this.children.unshift(child);
  }

  get lastElementChild() {
    return this.children[this.children.length - 1] || null;
  }

  replaceChildren(...children) {
    for (const child of children) {
      child.parentNode = this;
    }
    this.children = [...children];
  }

  remove() {
    if (!this.parentNode) {
      return;
    }
    this.parentNode.children = this.parentNode.children.filter((child) => child !== this);
  }

  focus() {
    this.focused = true;
  }

  cloneNode() {
    return new FakePostElement();
  }

  querySelector() {
    return new FakeElement("child");
  }
}

class FakeFormElement extends FakeElement {
  constructor(id, initialFields) {
    super(id);
    this.fieldValues = { ...initialFields };
    this.elements = Object.fromEntries(
      Object.keys(initialFields).map((name) => [name, new FakeInputElement(name, this)]),
    );
    this.resetCount = 0;
  }

  setFields(fields) {
    for (const [key, value] of Object.entries(fields)) {
      this.fieldValues[key] = value;
      if (!this.elements[key]) {
        this.elements[key] = new FakeInputElement(key, this);
      }
      this.elements[key].value = value;
    }
  }

  reset() {
    this.resetCount += 1;
    for (const key of Object.keys(this.fieldValues)) {
      this.fieldValues[key] = "";
      if (this.elements[key]) {
        this.elements[key].value = "";
      }
    }
  }
}

class FakeInputElement extends FakeElement {
  constructor(name, form) {
    super(name);
    this.name = name;
    this.form = form;
    this.value = form.fieldValues[name] || "";
  }
}

class FakeTemplateElement extends FakeElement {
  constructor(id) {
    super(id);
    this.content = {
      firstElementChild: new FakePostElement(),
    };
  }
}

class FakePostElement extends FakeElement {
  constructor() {
    super("post");
    this.parts = new Map();
  }

  querySelector(selector) {
    if (!this.parts.has(selector)) {
      this.parts.set(selector, new FakeElement(selector));
    }
    return this.parts.get(selector);
  }

  cloneNode() {
    return new FakePostElement();
  }
}

class FakeOption {
  constructor(text, value) {
    this.text = text;
    this.value = value;
  }
}

class FakeFormData {
  constructor(form) {
    this.form = form;
  }

  get(name) {
    return this.form.fieldValues[name] || "";
  }
}

class FakeFileReader {
  addEventListener() {}
  readAsDataURL() {}
}

class FakeWebSocket {
  addEventListener() {}
  send() {}
}

class FakeLocalStorage {
  constructor() {
    this.values = new Map();
  }

  getItem(key) {
    return this.values.get(key) || null;
  }

  setItem(key, value) {
    this.values.set(key, value);
  }

  removeItem(key) {
    this.values.delete(key);
  }
}

function bootstrap() {
  document = new FakeDocument();
  localStorage = new FakeLocalStorage();
  serviceCalls = [];
  localProfileIndex = 0;

  global.window = global;
  global.document = document;
  global.localStorage = localStorage;
  global.performance = { now: () => Date.now() };
  global.setTimeout = () => 0;
  global.clearTimeout = () => {};
  global.setInterval = () => 0;
  global.clearInterval = () => {};
  global.Option = FakeOption;
  global.FormData = FakeFormData;
  global.FileReader = FakeFileReader;
  global.AnonNetClient = FakeAnonNetClient;
  global.SocialSessionStore = class {};
  global.SocialService = FakeSocialService;
  global.SocialBackgroundSyncService = FakeBackgroundSyncService;
  global.WebSocket = class {};

  prepareDom();
  loadBrowserScript("social/models.js");
  loadBrowserScript("social/runtime.js");
  loadBrowserScript("state.js");
  loadBrowserScript("app.js");
}

bootstrap();
main().catch((error) => {
  console.error(`social DOM smoke failed: ${error.stack || error.message}`);
  process.exitCode = 1;
});
