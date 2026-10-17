import { useQuery, useQueryClient } from "@tanstack/react-query";
import { type DragEvent, type KeyboardEvent, useMemo, useRef, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { api, toAppError } from "../../../api/client";
import type { AppError, ConnectionFolder, TreeItemRef } from "../../../api/types";
import { tr } from "../../../i18n/text";
import { useWorkspace } from "../../../state/workspace";
import { ErrorBanner } from "../../common/ErrorBanner";
import { ConnectionNode } from "./ConnectionNode";
import { FolderMenu, type FolderMenuItem } from "./FolderMenu";
import { buildTree, canMoveFolder, filterTree, type TreeNode } from "./folderTree";

const COLLAPSED_KEY = "glafira.tree.collapsed";
/** Тип данных перетаскивания: сам элемент берётся из ref (в dragover браузер данные не отдаёт). */
const DRAG_TYPE = "application/x-glafira-tree-item";

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

/** Ввод имени папки прямо в дереве: Enter — сохранить, Esc — отмена, ошибка Rust — под полем. */
function FolderNameInput({ initial, onSubmit, onCancel }: { initial: string; onSubmit: (name: string) => Promise<unknown>; onCancel: () => void }) {
  const { t } = useTranslation();
  const [value, setValue] = useState(initial);
  const [error, setError] = useState<AppError | null>(null);
  const [busy, setBusy] = useState(false);

  const onKeyDown = async (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Escape") {
      e.preventDefault();
      if (!busy) onCancel();
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (busy) return;
      setBusy(true);
      setError(null);
      try {
        await onSubmit(value.trim());
      } catch (err) {
        setError(toAppError(err));
      } finally {
        setBusy(false);
      }
    }
  };

  return (
    <div className="tree-name-input">
      <input
        autoFocus
        aria-label={t("sidebar.folder.namePlaceholder")}
        placeholder={t("sidebar.folder.namePlaceholder")}
        value={value}
        readOnly={busy}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={onKeyDown}
      />
      {error && (
        <p className="tree-error" role="alert">
          {tr(error)}
        </p>
      )}
    </div>
  );
}

/** Подтверждение удаления папки внутри страницы (window.confirm в Tauri не работает). */
function DeleteFolderConfirm({ folder, onConfirm, onCancel }: { folder: ConnectionFolder; onConfirm: () => Promise<unknown>; onCancel: () => void }) {
  const { t } = useTranslation();
  const [error, setError] = useState<AppError | null>(null);
  const [busy, setBusy] = useState(false);
  const title = t("sidebar.folder.deleteConfirm", { name: folder.name });

  const confirm = async () => {
    setBusy(true);
    setError(null);
    try {
      await onConfirm();
    } catch (err) {
      setError(toAppError(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="confirm tree-confirm" role="group" aria-label={title} onKeyDown={(e) => e.key === "Escape" && !busy && onCancel()}>
      <strong>{title}</strong>
      {error && (
        <p className="tree-error" role="alert">
          {tr(error)}
        </p>
      )}
      <div className="toolbar">
        <button type="button" className="danger" disabled={busy} onClick={confirm} autoFocus>
          {t("sidebar.folder.delete")}
        </button>
        <button type="button" disabled={busy} onClick={onCancel}>
          {t("common.cancel")}
        </button>
      </div>
    </div>
  );
}

type Editing = { mode: "create"; parentId: string | null } | { mode: "rename"; folderId: string };
/** Куда бросают: внутрь папки, перед подключением или в корень. */
type DropTarget = { key: string; parentId: string | null; before: TreeItemRef | null };

/** Дерево подключений в левой панели: заголовок с действиями, фильтр, папки и подключения. */
export function ConnectionTree() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const open = useWorkspace((s) => s.open);
  const connectionsQ = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const foldersQ = useQuery({ queryKey: ["folders"], queryFn: api.listFolders });
  const [query, setQuery] = useState("");
  const [collapsed, setCollapsed] = useState(readCollapsed);
  /** Открытое меню: id папки или "header" для меню заголовка. */
  const [menu, setMenu] = useState<string | null>(null);
  const [editing, setEditing] = useState<Editing | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [dropKey, setDropKey] = useState<string | null>(null);
  const [moveError, setMoveError] = useState<unknown>(null);
  const dragged = useRef<TreeItemRef | null>(null);

  const connections = connectionsQ.data;
  const folders = foldersQ.data;
  const tree = useMemo(() => buildTree(folders ?? [], connections ?? []), [folders, connections]);
  const visible = useMemo(() => filterTree(tree, query), [tree, query]);
  const filtering = query.trim() !== "";

  const refresh = () =>
    Promise.all([
      queryClient.invalidateQueries({ queryKey: ["folders"] }),
      queryClient.invalidateQueries({ queryKey: ["connections"] }),
    ]);

  const setCollapsedAndSave = (update: (next: Set<string>) => void) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      update(next);
      writeCollapsed(next);
      return next;
    });

  const toggle = (id: string) =>
    setCollapsedAndSave((next) => {
      if (!next.delete(id)) next.add(id);
    });

  const toggleMenu = (id: string) => setMenu((m) => (m === id ? null : id));

  const startCreate = (parentId: string | null) => {
    setDeleting(null);
    setEditing({ mode: "create", parentId });
    // Новая строка появляется внутри родителя — он должен быть развёрнут.
    if (parentId !== null && collapsed.has(parentId)) setCollapsedAndSave((next) => next.delete(parentId));
  };

  const createFolder = async (name: string, parentId: string | null) => {
    await api.createFolder(name, parentId);
    setEditing(null);
    await refresh();
  };

  const renameFolder = async (id: string, name: string) => {
    await api.renameFolder(id, name);
    setEditing(null);
    await refresh();
  };

  const deleteFolder = async (id: string) => {
    await api.deleteFolder(id);
    setDeleting(null);
    await refresh();
  };

  const folderMenuItems = (folder: ConnectionFolder): FolderMenuItem[] => [
    { label: t("sidebar.newFolder"), onSelect: () => startCreate(folder.id) },
    { label: t("sidebar.folder.newConnection"), onSelect: () => open({ kind: "connection-form", connectionId: null, folderId: folder.id }) },
    {
      label: t("sidebar.folder.rename"),
      onSelect: () => {
        setDeleting(null);
        setEditing({ mode: "rename", folderId: folder.id });
      },
    },
    {
      label: t("sidebar.folder.delete"),
      onSelect: () => {
        setEditing(null);
        setDeleting(folder.id);
      },
    },
  ];

  // --- Перетаскивание ---
  const canDrop = (item: TreeItemRef | null, target: DropTarget): boolean => {
    if (!item) return false;
    if (item.kind === "folder") return canMoveFolder(folders ?? [], item.id, target.parentId);
    return !(target.before?.kind === "connection" && target.before.id === item.id);
  };

  const dragSource = (item: TreeItemRef) => ({
    draggable: true,
    onDragStart: (e: DragEvent) => {
      e.stopPropagation();
      dragged.current = item;
      setMoveError(null);
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData(DRAG_TYPE, JSON.stringify(item));
    },
    onDragEnd: () => {
      dragged.current = null;
      setDropKey(null);
    },
  });

  const dropTarget = (target: DropTarget) => ({
    onDragOver: (e: DragEvent) => {
      e.stopPropagation();
      // Без preventDefault браузер не разрешает drop: недопустимая цель так и остаётся без подсветки.
      if (!canDrop(dragged.current, target)) {
        if (dropKey === target.key) setDropKey(null);
        return;
      }
      e.preventDefault();
      e.dataTransfer.dropEffect = "move";
      if (dropKey !== target.key) setDropKey(target.key);
    },
    onDragLeave: (e: DragEvent) => {
      e.stopPropagation();
      if (dropKey === target.key) setDropKey(null);
    },
    onDrop: (e: DragEvent) => {
      e.stopPropagation();
      const item = dragged.current;
      dragged.current = null;
      setDropKey(null);
      if (!item || !canDrop(item, target)) return;
      e.preventDefault();
      api
        .moveItems([item], target.parentId, target.before)
        .catch((err: unknown) => setMoveError(err))
        .finally(() => void refresh());
    },
  });

  const rootTarget = dropTarget({ key: "root", parentId: null, before: null });

  const nameInput = (parentId: string | null) =>
    editing?.mode === "create" && editing.parentId === parentId ? (
      <li key="new-folder">
        <FolderNameInput initial="" onSubmit={(name) => createFolder(name, parentId)} onCancel={() => setEditing(null)} />
      </li>
    ) : null;

  const renderNodes = (nodes: TreeNode[], parentId: string | null) =>
    nodes.map((node) => {
      if (node.kind === "connection") {
        const c = node.connection;
        const key = `c:${c.id}`;
        return (
          <ConnectionNode
            key={c.id}
            connection={c}
            rowProps={{
              ...dragSource({ kind: "connection", id: c.id }),
              ...dropTarget({ key, parentId, before: { kind: "connection", id: c.id } }),
              className: dropKey === key ? "drop-before" : undefined,
            }}
          />
        );
      }
      const { folder } = node;
      const key = `f:${folder.id}`;
      const expanded = filtering || !collapsed.has(folder.id);
      const renaming = editing?.mode === "rename" && editing.folderId === folder.id;
      const childInput = nameInput(folder.id);
      return (
        <li key={folder.id}>
          {renaming ? (
            <FolderNameInput initial={folder.name} onSubmit={(name) => renameFolder(folder.id, name)} onCancel={() => setEditing(null)} />
          ) : (
            <div
              className={dropKey === key ? "tree-folder drop-into" : "tree-folder"}
              {...dragSource({ kind: "folder", id: folder.id })}
              {...dropTarget({ key, parentId: folder.id, before: null })}
              onContextMenu={(e) => {
                e.preventDefault();
                setMenu(folder.id);
              }}
            >
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
              <span className="tree-menu-anchor">
                <button type="button" className="icon" title={t("sidebar.menu")} aria-label={t("sidebar.menu")} onClick={() => toggleMenu(folder.id)}>
                  ⋯
                </button>
                {menu === folder.id && <FolderMenu label={t("sidebar.menu")} items={folderMenuItems(folder)} onClose={() => setMenu(null)} />}
              </span>
            </div>
          )}
          {deleting === folder.id && <DeleteFolderConfirm folder={folder} onConfirm={() => deleteFolder(folder.id)} onCancel={() => setDeleting(null)} />}
          {expanded && (childInput || node.children.length > 0) && (
            <ul>
              {childInput}
              {renderNodes(node.children, folder.id)}
            </ul>
          )}
        </li>
      );
    });

  const error = connectionsQ.error ?? foldersQ.error;
  const empty = connectionsQ.isSuccess && (connections ?? []).length === 0 && (folders ?? []).length === 0;

  return (
    <>
      <div className={dropKey === "root" ? "sidebar-header drop-into" : "sidebar-header"} {...rootTarget}>
        <span>{t("sidebar.connections")}</span>
        <span className="sidebar-actions">
          <button type="button" title={t("sidebar.newConnection")} onClick={() => open({ kind: "connection-form", connectionId: null })}>
            +
          </button>
          <button type="button" title={t("sidebar.newFolder")} aria-label={t("sidebar.newFolder")} onClick={() => startCreate(null)}>
            +📁
          </button>
          <span className="tree-menu-anchor">
            <button type="button" title={t("sidebar.menu")} aria-label={t("sidebar.menu")} onClick={() => toggleMenu("header")}>
              ⋯
            </button>
            {menu === "header" && (
              <FolderMenu label={t("sidebar.menu")} items={[{ label: t("sidebar.newFolder"), onSelect: () => startCreate(null) }]} onClose={() => setMenu(null)} />
            )}
          </span>
        </span>
      </div>
      {error && <ErrorBanner error={error} />}
      {moveError != null && <ErrorBanner error={moveError} />}
      {empty && !editing && (
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
      <ul className="tree">
        {nameInput(null)}
        {renderNodes(visible, null)}
      </ul>
      <div className={dropKey === "root" ? "tree-root-drop drop-into" : "tree-root-drop"} aria-hidden="true" {...rootTarget} />
    </>
  );
}
