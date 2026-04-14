/* ResearchDB — Main JS */

// ─── Sidebar Toggle ──────────────────────────────────────────────────────────
const sidebar     = document.getElementById('sidebar');
const mainWrapper = document.getElementById('mainWrapper');
const toggleBtn   = document.getElementById('sidebarToggle');
const overlay     = document.getElementById('sidebarOverlay');

const isMobile = () => window.innerWidth < 769;

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
if (overlay) overlay.addEventListener('click', closeSidebar);

// Restore desktop collapsed state
if (!isMobile() && localStorage.getItem('sidebarCollapsed') === 'true') {
  document.body.classList.add('sidebar-collapsed');
}

// ─── Dark Mode ────────────────────────────────────────────────────────────────
const html        = document.documentElement;
const themeToggle = document.getElementById('themeToggle');

function applyTheme(dark) {
  html.setAttribute('data-theme', dark ? 'dark' : 'light');
  // Update Chart.js defaults
  if (window.Chart) {
    Chart.defaults.color = dark ? '#94a3b8' : '#64748b';
    Chart.defaults.borderColor = dark ? '#334155' : '#e2e8f0';
  }
  // Re-render existing chart instances so center label + grid colors update
  if (window.wardDonutChart) {
    window.wardDonutChart.update();
  }
}

const savedTheme = localStorage.getItem('theme');
const isDark = savedTheme === 'dark';
applyTheme(isDark);

if (themeToggle) {
  themeToggle.addEventListener('click', () => {
    const dark = html.getAttribute('data-theme') !== 'dark';
    localStorage.setItem('theme', dark ? 'dark' : 'light');
    applyTheme(dark);
  });
}

// ─── Count-up animation ───────────────────────────────────────────────────────
function countUp(el, target, duration) {
  if (target === 0) return;
  const start = performance.now();
  const step = (now) => {
    const progress = Math.min((now - start) / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3); // ease-out cubic
    el.textContent = Math.round(eased * target).toLocaleString();
    if (progress < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

document.querySelectorAll('.stat-number').forEach(el => {
  const raw = parseInt(el.textContent.replace(/[^0-9]/g, ''), 10);
  if (!isNaN(raw) && raw > 0) {
    // Vary duration by magnitude for a natural feel
    const duration = Math.min(400 + raw * 0.002, 1400);
    el.textContent = '0';
    countUp(el, raw, duration);
  }
});

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
