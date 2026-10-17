import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { api } from "../../../api/client";
import { ErrorBanner } from "../../common/ErrorBanner";
import { ConnectionNode } from "./ConnectionNode";
import { buildTree, filterTree, type TreeNode } from "./folderTree";

const COLLAPSED_KEY = "glafira.tree.collapsed";

function readCollapsed(): Set<string> {
  try {
    const raw = localStorage.getItem(COLLAPSED_KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : [];
    return new Set(Array.isArray(parsed) ? parsed.filter((x): x is string => typeof x === "string") : []);
  } catch {
    return new Set();
  }
}

function writeCollapsed(ids: Set<string>) {
  try {
    localStorage.setItem(COLLAPSED_KEY, JSON.stringify([...ids]));
  } catch {
    // Хранилище недоступно — состояние живёт только до перезапуска.
  }
}

/** Дерево подключений в левой панели: фильтр, папки (сворачиваются) и подключения. */
export function ConnectionTree() {
  const { t } = useTranslation();
  const connectionsQ = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const foldersQ = useQuery({ queryKey: ["folders"], queryFn: api.listFolders });
  const [query, setQuery] = useState("");
  const [collapsed, setCollapsed] = useState(readCollapsed);

  const connections = connectionsQ.data;
  const folders = foldersQ.data;
  const tree = useMemo(() => buildTree(folders ?? [], connections ?? []), [folders, connections]);
  const visible = useMemo(() => filterTree(tree, query), [tree, query]);
  const filtering = query.trim() !== "";

  const toggle = (id: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (!next.delete(id)) next.add(id);
      writeCollapsed(next);
      return next;
    });
  };

  const renderNodes = (nodes: TreeNode[]) =>
    nodes.map((node) => {
      if (node.kind === "connection") return <ConnectionNode key={node.connection.id} connection={node.connection} />;
      const { folder } = node;
      const expanded = filtering || !collapsed.has(folder.id);
      return (
        <li key={folder.id}>
          <div className="tree-folder">
            <button
              type="button"
              className="tree-folder-toggle"
              aria-expanded={expanded}
              aria-label={t("sidebar.folderToggle", { name: folder.name })}
              onClick={() => toggle(folder.id)}
            >
              <span className="tree-caret" aria-hidden="true">
                {expanded ? "▾" : "▸"}
              </span>
              <span className="tree-name">{folder.name}</span>
            </button>
          </div>
          {expanded && node.children.length > 0 && <ul>{renderNodes(node.children)}</ul>}
        </li>
      );
    });

  const error = connectionsQ.error ?? foldersQ.error;
  const empty = connectionsQ.isSuccess && (connections ?? []).length === 0 && (folders ?? []).length === 0;

  return (
    <>
      {error && <ErrorBanner error={error} />}
      {empty && (
        <p className="empty">
          <Trans i18nKey="sidebar.emptyHint" components={{ code: <code /> }} />
        </p>
      )}
      {tree.length > 0 && (
        <input
          type="search"
          className="tree-filter"
          placeholder={t("sidebar.filter")}
          aria-label={t("sidebar.filter")}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      )}
      {filtering && visible.length === 0 && <p className="empty">{t("sidebar.noMatches")}</p>}
      <ul className="tree">{renderNodes(visible)}</ul>
    </>
  );
}
