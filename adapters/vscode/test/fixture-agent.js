"use strict";
// Synthetic ACP stdio peer. Never connects to a model or external service.
const readline = require("node:readline");
const send = value => process.stdout.write(JSON.stringify({jsonrpc: "2.0", ...value}) + "\n");
const update = value => send({method: "session/update", params: {sessionId: "synthetic-session", update: value}});
let waiting;
readline.createInterface({input: process.stdin}).on("line", line => {
  const frame = JSON.parse(line);
  const result = value => send({id: frame.id, result: value});
  if (frame.method === "initialize") result({protocolVersion: 1, agentCapabilities: {loadSession: true}});
  else if (frame.method === "session/new") result({sessionId: "synthetic-session"});
  else if (frame.method === "session/load") {
    update({sessionUpdate: "tool_call", toolCallId: "synthetic-call", status: "completed"});
    result({});
  } else if (frame.method === "session/prompt") {
    const text = frame.params.prompt[0].text;
    if (text === "disconnect") process.exit(0);
    if (text === "wait") { waiting = frame.id; return; }
    if (text === "wrong-session") {
      send({method: "session/update", params: {sessionId: "foreign",
        update: {sessionUpdate: "plan", entries: []}}});
    } else {
      update({sessionUpdate: "tool_call", toolCallId: "synthetic-call", status: "pending"});
      update({sessionUpdate: "tool_call_update", toolCallId: "synthetic-call", status: "completed"});
      update({sessionUpdate: "tool_call_update", toolCallId: "synthetic-call", status: "completed"});
      update({sessionUpdate: "plan", entries: [
        {content: "synthetic private plan text", priority: "high", status: "completed"}]});
      update({sessionUpdate: "agent_message_chunk", content: {type: "text", text: "Synthetic response"}});
    }
    result({stopReason: "end_turn"});
  } else if (frame.method === "session/cancel" && waiting) {
    send({id: waiting, result: {stopReason: "cancelled"}}); waiting = undefined;
  }
});
