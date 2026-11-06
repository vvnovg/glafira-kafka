import type { ReactNode, SVGAttributes } from "react";

/**
 * Набор значков интерфейса (спец. U1 §5): SVG 16×16, обводка 1.5, цвет `currentColor`. Значок без подписи
 * всегда лежит внутри кнопки с `aria-label`; сам он скрыт от экранного диктора.
 */
export type IconName =
  | "plus"
  | "folderPlus"
  | "folder"
  | "more"
  | "pencil"
  | "close"
  | "chevron"
  | "warning"
  | "lock"
  | "help"
  | "refresh"
  | "send"
  | "users"
  | "play"
  | "stop"
  | "copy"
  | "search"
  | "settings"
  | "heart"
  | "book"
  | "info"
  | "grid"
  | "list"
  | "flag"
  | "dot";

const PATHS: Record<IconName, ReactNode> = {
  plus: <path d="M8 3v10M3 8h10" />,
  folderPlus: (
    <>
      <path d="M2 4.5h4l1.5 1.5H14v7.5H2z" />
      <path d="M8 8v4M6 10h4" />
    </>
  ),
  folder: <path d="M2 4.5h4l1.5 1.5H14v7.5H2z" />,
  more: (
    <>
      <circle cx="3.5" cy="8" r="1.4" fill="currentColor" stroke="none" />
      <circle cx="8" cy="8" r="1.4" fill="currentColor" stroke="none" />
      <circle cx="12.5" cy="8" r="1.4" fill="currentColor" stroke="none" />
    </>
  ),
  pencil: <path d="M11 2.5l2.5 2.5L5.5 13H3v-2.5z" />,
  close: <path d="M4 4l8 8M12 4l-8 8" />,
  chevron: <path d="M6 4l4 4-4 4" />,
  warning: (
    <>
      <path d="M8 2.5l6 11H2z" />
      <path d="M8 6.5v3.2" />
      <circle cx="8" cy="11.6" r=".6" fill="currentColor" stroke="none" />
    </>
  ),
  lock: (
    <>
      <rect x="3" y="7" width="10" height="7" rx="1.5" />
      <path d="M5.5 7V5a2.5 2.5 0 0 1 5 0v2" />
    </>
  ),
  help: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="M6.3 6.3a1.7 1.7 0 1 1 2.6 1.4c-.6.4-.9.8-.9 1.3" />
      <circle cx="8" cy="11.3" r=".6" fill="currentColor" stroke="none" />
    </>
  ),
  refresh: (
    <>
      <path d="M13 8a5 5 0 1 1-1.6-3.7" />
      <path d="M13 2.5v3h-3" />
    </>
  ),
  send: <path d="M2.5 8L14 2.5 10 13.5 8 9z" />,
  users: (
    <>
      <circle cx="6" cy="5.5" r="2.3" />
      <path d="M1.8 13c.4-2.3 2.1-3.5 4.2-3.5s3.8 1.2 4.2 3.5" />
      <circle cx="11.5" cy="5.5" r="1.8" />
      <path d="M11 9.6c1.9 0 3 1.1 3.3 3" />
    </>
  ),
  play: <path d="M4 2.5v11l9-5.5z" fill="currentColor" stroke="none" />,
  stop: <rect x="3" y="3" width="10" height="10" rx="1.5" fill="currentColor" stroke="none" />,
  copy: (
    <>
      <rect x="5" y="5" width="8" height="8" rx="1.5" />
      <path d="M3 11V4a1 1 0 0 1 1-1h7" />
    </>
  ),
  search: (
    <>
      <circle cx="7" cy="7" r="4.5" />
      <path d="M10.5 10.5L14 14" />
    </>
  ),
  settings: (
    <>
      <path d="M2 4.5h12M2 8h12M2 11.5h12" />
      <circle cx="6" cy="4.5" r="1.5" fill="var(--sidebar, var(--bg))" />
      <circle cx="11" cy="8" r="1.5" fill="var(--sidebar, var(--bg))" />
      <circle cx="5" cy="11.5" r="1.5" fill="var(--sidebar, var(--bg))" />
    </>
  ),
  heart: <path d="M8 13.5S2.5 10 2.5 6.2A2.9 2.9 0 0 1 8 4.6a2.9 2.9 0 0 1 5.5 1.6C13.5 10 8 13.5 8 13.5z" />,
  book: <path d="M2.5 3h4a1.5 1.5 0 0 1 1.5 1.5V13a1.5 1.5 0 0 0-1.5-1.5h-4zM13.5 3h-4A1.5 1.5 0 0 0 8 4.5V13a1.5 1.5 0 0 1 1.5-1.5h4z" />,
  info: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="M8 7v4" />
      <circle cx="8" cy="5" r=".6" fill="currentColor" stroke="none" />
    </>
  ),
  grid: (
    <>
      <rect x="2.5" y="2.5" width="5" height="5" rx="1" />
      <rect x="8.5" y="2.5" width="5" height="5" rx="1" />
      <rect x="2.5" y="8.5" width="5" height="5" rx="1" />
      <rect x="8.5" y="8.5" width="5" height="5" rx="1" />
    </>
  ),
  list: <path d="M2.5 4h11M2.5 8h11M2.5 12h7" />,
  flag: (
    <>
      <path d="M4 2.5v11" />
      <path d="M4 3h8l-2 3 2 3H4" />
    </>
  ),
  dot: <circle cx="8" cy="8" r="4" fill="currentColor" stroke="none" />,
};

export const ICON_NAMES = Object.keys(PATHS) as IconName[];

interface Props extends Omit<SVGAttributes<SVGSVGElement>, "children"> {
  name: IconName;
  size?: number;
}

export function Icon({ name, size = 16, className, ...rest }: Props) {
  return (
    <svg
      className={className ? `icon-svg ${className}` : "icon-svg"}
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
      data-icon={name}
      {...rest}
    >
      {PATHS[name]}
    </svg>
  );
}
