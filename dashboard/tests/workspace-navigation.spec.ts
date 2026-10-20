import {test, expect} from "@playwright/test";
import {isProjectView, viewPath} from "../src/view-state";

test("workspace features reserve project routes rather than masquerading as agent IDs", () => {
  for (const view of ["tasks", "coordination", "context", "operations", "privacy"]) {
    expect(isProjectView(view)).toBe(true);
    expect(viewPath("project-1", view)).toBe(`/projects/project-1/${view}`);
  }
  expect(isProjectView("agent-1")).toBe(false);
  expect(viewPath("project-1", "agent-1")).toBe("/projects/project-1/agents/agent-1");
});

test("workspace route builders encode identities and preserve existing views", () => {
  expect(viewPath("project/name", "tasks")).toBe("/projects/project%2Fname/tasks");
  expect(viewPath("project-1", "agent/name")).toBe("/projects/project-1/agents/agent%2Fname");
  for (const view of ["board", "conversations", "memories", "administration"]) {
    expect(viewPath("project-1", view)).toBe(`/projects/project-1/${view}`);
  }
});
