/* ResearchDB — Main JS */

// ─── Sidebar Toggle ──────────────────────────────────────────────────────────
const sidebar       = document.getElementById('sidebar');
const mainWrapper   = document.getElementById('mainWrapper');
const toggleBtn     = document.getElementById('sidebarToggle');
const overlay       = document.getElementById('sidebarOverlay');

const isMobile = () => window.innerWidth < 768;

function openSidebar() {
  sidebar.classList.add('open');
  overlay.classList.add('active');
}
function closeSidebar() {
  sidebar.classList.remove('open');
  overlay.classList.remove('active');
}
function toggleDesktopSidebar() {
  document.body.classList.toggle('sidebar-collapsed');
  const collapsed = document.body.classList.contains('sidebar-collapsed');
  localStorage.setItem('sidebarCollapsed', collapsed);
}

if (toggleBtn) {
  toggleBtn.addEventListener('click', () => {
    if (isMobile()) {
      sidebar.classList.contains('open') ? closeSidebar() : openSidebar();
    } else {
      toggleDesktopSidebar();
    }
  });
}
if (overlay) {
  overlay.addEventListener('click', closeSidebar);
}

// Restore desktop collapsed state
if (!isMobile() && localStorage.getItem('sidebarCollapsed') === 'true') {
  document.body.classList.add('sidebar-collapsed');
}

// ─── Auto-dismiss alerts ─────────────────────────────────────────────────────
document.querySelectorAll('.alert').forEach(el => {
  setTimeout(() => {
    const bsAlert = bootstrap.Alert.getOrCreateInstance(el);
    if (bsAlert) bsAlert.close();
  }, 5000);
});

// ─── Tooltip init ─────────────────────────────────────────────────────────────
document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
  new bootstrap.Tooltip(el);
});

// ─── Number formatting for stat cards ────────────────────────────────────────
document.querySelectorAll('.stat-number').forEach(el => {
  const raw = parseInt(el.textContent.replace(/,/g, ''), 10);
  if (!isNaN(raw)) el.textContent = raw.toLocaleString();
});
