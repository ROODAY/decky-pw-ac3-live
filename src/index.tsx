import { ButtonItem, PanelSection, PanelSectionRow, staticClasses } from "@decky/ui";
import {
  addEventListener,
  removeEventListener,
  callable,
  definePlugin,
  toaster,
} from "@decky/api";
import { useState } from "react";
import { FaVolumeUp } from "react-icons/fa";

// Diagnostic backend call used to validate frontend/backend connectivity.
const add = callable<[first: number, second: number], number>("add");

// Backend diagnostic event trigger.
const startTimer = callable<[], void>("start_timer");

function Content() {
  const [result, setResult] = useState<number | undefined>();

  const onClick = async () => {
    const value = await add(Math.random(), Math.random());
    setResult(value);
  };

  return (
    <PanelSection title="pw-ac3-live">
      <PanelSectionRow>
        <ButtonItem layout="below" onClick={onClick}>
          {result ?? "Backend health check"}
        </ButtonItem>
      </PanelSectionRow>
      <PanelSectionRow>
        <ButtonItem layout="below" onClick={() => startTimer()}>
          Emit backend event
        </ButtonItem>
      </PanelSectionRow>
    </PanelSection>
  );
}

export default definePlugin(() => {
  console.log("pw-ac3-live plugin initializing");

  const listener = addEventListener<[
    message: string,
    success: boolean,
    code: number,
  ]>("timer_event", (message, success, code) => {
    toaster.toast({
      title: "pw-ac3-live backend event",
      body: `${message}, ${success}, ${code}`,
    });
  });

  return {
    name: "pw-ac3-live",
    titleView: <div className={staticClasses.Title}>pw-ac3-live</div>,
    content: <Content />,
    icon: <FaVolumeUp />,
    onDismount() {
      console.log("pw-ac3-live plugin unloading");
      removeEventListener("timer_event", listener);
    },
  };
});
