import React from "react";
import * as Avatar from "@radix-ui/react-avatar";
import * as Tooltip from "@radix-ui/react-tooltip";

export function AgentAvatar({name, appearance}: {name: string; appearance: string}) {
  const initials = name.trim().split(/\s+/).slice(0, 2).map(part => Array.from(part)[0]).join("");
  return <Avatar.Root className="agent-avatar" data-appearance={appearance} aria-hidden="true">
    <Avatar.Fallback>{initials || "A"}</Avatar.Fallback>
  </Avatar.Root>;
}

export function ControlTooltip({text, children}: {text: string; children: React.ReactElement}) {
  return <Tooltip.Root>
    <Tooltip.Trigger asChild>{children}</Tooltip.Trigger>
    <Tooltip.Portal>
      <Tooltip.Content className="control-tooltip" sideOffset={8} collisionPadding={12}>
        {text}<Tooltip.Arrow className="control-tooltip-arrow"/>
      </Tooltip.Content>
    </Tooltip.Portal>
  </Tooltip.Root>;
}

export const TooltipProvider = Tooltip.Provider;
