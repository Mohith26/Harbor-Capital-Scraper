/* Harbor Capital Comp Database — Core JS */

// ===== HTMX + Alpine.js Integration =====
document.addEventListener('htmx:afterSwap', function(e) {
    if (window.Alpine) {
        Alpine.initTree(e.detail.target);
    }
});

// ===== AG Grid Helpers =====
let _gridApi = null;
let _gridColumnsSig = null;

function _columnsSignature(columnDefs) {
    return columnDefs.map(c => c.field).join('|');
}

function initGrid(containerId, columnDefs, rowData, options) {
    const container = document.getElementById(containerId);
    if (!container) return null;

    const defaultOptions = {
        columnDefs: columnDefs,
        rowData: rowData,
        defaultColDef: {
            sortable: true, filter: true, resizable: true, minWidth: 100,
        },
        rowSelection: 'multiple',
        animateRows: false,
        pagination: true,
        paginationPageSize: 50,
        domLayout: 'autoHeight',
        suppressColumnVirtualisation: false,
        ...options,
    };

    const newSig = _columnsSignature(columnDefs);

    // Fast path: grid exists + same columns → just update data
    if (_gridApi && _gridColumnsSig === newSig) {
        try {
            _gridApi.setGridOption('rowData', rowData);
            return _gridApi;
        } catch (e) {
            // Fall through to recreate if setGridOption fails
        }
    }

    // Slow path: columns changed or no grid → destroy and recreate
    if (_gridApi) {
        try { _gridApi.destroy(); } catch (e) {}
        _gridApi = null;
    }
    container.innerHTML = '';  // clear old DOM
    _gridApi = agGrid.createGrid(container, defaultOptions);
    _gridColumnsSig = newSig;
    return _gridApi;
}

function updateGridData(rowData) {
    if (_gridApi) {
        _gridApi.setGridOption('rowData', rowData);
    }
}

function getSelectedIds() {
    if (!_gridApi) return [];
    return _gridApi.getSelectedRows().map(r => r.id);
}

// ===== Plotly Helpers =====
const HC_COLORS = {
    amber: '#F5A623',
    charcoal: '#333333',
    amberPale: '#FFF3DC',
    bg: '#f4f5f7',
    series: ['#F5A623', '#333333', '#6B7280', '#3B82F6', '#10B981', '#EF4444', '#8B5CF6']
};

const HC_LAYOUT = {
    font: { family: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif', size: 12 },
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    margin: { l: 50, r: 20, t: 40, b: 40 },
    colorway: HC_COLORS.series,
};

function renderChart(divId, traces, layoutOverrides) {
    const layout = { ...HC_LAYOUT, ...layoutOverrides };
    Plotly.newPlot(divId, traces, layout, { responsive: true, displayModeBar: false });
}

// ===== Leaflet Helpers =====
let _maps = {};

function initMap(divId, center, zoom) {
    if (_maps[divId]) {
        _maps[divId].remove();
    }
    const map = L.map(divId, { preferCanvas: true }).setView(center || [29.76, -95.37], zoom || 10);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors',
        maxZoom: 19,
    }).addTo(map);
    _maps[divId] = map;
    return map;
}

function addMarkers(map, points, options) {
    const markers = [];
    points.forEach(function(pt) {
        if (pt.lat && pt.lng) {
            const marker = L.circleMarker([pt.lat, pt.lng], {
                radius: options?.radius || 6,
                fillColor: options?.color || HC_COLORS.amber,
                color: '#fff',
                weight: 1,
                fillOpacity: 0.8,
            }).addTo(map);
            if (pt.popup) marker.bindPopup(pt.popup);
            markers.push(marker);
        }
    });
    if (markers.length > 0) {
        const group = L.featureGroup(markers);
        map.fitBounds(group.getBounds().pad(0.1));
    }
    return markers;
}

// ===== Export Helper =====
async function exportData(url, format, compType) {
    const ids = getSelectedIds();
    const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ format: format, ids: ids, type: compType }),
    });
    if (!response.ok) { alert('Export failed'); return; }
    const blob = await response.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    const ext = format === 'xlsx' ? 'xlsx' : format === 'kml' ? 'kml' : 'csv';
    a.download = compType + '_comps.' + ext;
    a.click();
    URL.revokeObjectURL(a.href);
}

// ===== Address Autocomplete (server-side Places proxy, Texas-biased) =====
async function _fetchAutocomplete(q) {
    try {
        const resp = await fetch('/api/autocomplete?q=' + encodeURIComponent(q));
        if (!resp.ok) return [];
        const data = await resp.json();
        return data.predictions || [];
    } catch (e) {
        return [];
    }
}

async function _fetchDbAutocomplete(q, opts) {
    opts = opts || {};
    try {
        const p = new URLSearchParams({ q: q });
        if (opts.type) p.set('type', opts.type);
        if (opts.fields) p.set('fields', opts.fields);
        const resp = await fetch('/api/db-autocomplete?' + p.toString());
        if (!resp.ok) return [];
        const data = await resp.json();
        return data.suggestions || [];
    } catch (e) {
        return [];
    }
}

function initAddressAutocomplete(input) {
    if (!input || input._autocompleteInitialized) return;
    input._autocompleteInitialized = true;
    input.removeAttribute('readonly');
    input.removeAttribute('disabled');
    input.setAttribute('autocomplete', 'off');

    // Build dropdown element
    const dropdown = document.createElement('div');
    dropdown.className = 'hc-autocomplete-dropdown';
    dropdown.style.cssText = 'position: absolute; z-index: 10000; background: #fff; border: 1px solid #e5e7eb; border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.12); max-height: 240px; overflow-y: auto; display: none; min-width: 200px;';
    document.body.appendChild(dropdown);

    let debounceTimer = null;
    let currentPredictions = [];
    let selectedIdx = -1;

    const positionDropdown = () => {
        const rect = input.getBoundingClientRect();
        dropdown.style.left = (rect.left + window.scrollX) + 'px';
        dropdown.style.top = (rect.bottom + window.scrollY + 2) + 'px';
        dropdown.style.width = rect.width + 'px';
    };

    const hideDropdown = () => { dropdown.style.display = 'none'; selectedIdx = -1; };

    const renderDropdown = () => {
        if (!currentPredictions.length) { hideDropdown(); return; }
        dropdown.innerHTML = currentPredictions.map((p, i) => {
            const badge = p.source === 'db'
                ? '<span style="margin-left:8px;font-size:10px;font-weight:600;color:#D88A10;background:#FFF3DC;padding:2px 6px;border-radius:8px;">In DB</span>'
                : '';
            return `
            <div class="hc-ac-item" data-idx="${i}"
                 style="padding: 8px 12px; font-size: 13px; cursor: pointer; border-bottom: 1px solid #f3f4f6; display:flex; align-items:center; justify-content:space-between; ${i === selectedIdx ? 'background: #FFF3DC;' : ''}">
                <span>${p.description.replace(/</g, '&lt;')}</span>${badge}
            </div>`;
        }).join('');
        positionDropdown();
        dropdown.style.display = 'block';

        dropdown.querySelectorAll('.hc-ac-item').forEach(el => {
            el.addEventListener('mousedown', (e) => {
                e.preventDefault();
                const idx = parseInt(el.dataset.idx, 10);
                const pick = currentPredictions[idx];
                if (pick) {
                    input.value = pick.description;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                }
                hideDropdown();
            });
        });
    };

    input.addEventListener('input', () => {
        const q = input.value.trim();
        clearTimeout(debounceTimer);
        if (q.length < 2) { hideDropdown(); return; }
        debounceTimer = setTimeout(async () => {
            // Fetch Google Places and DB-backed addresses in parallel
            const [googleRes, dbRes] = await Promise.all([
                q.length >= 3 ? _fetchAutocomplete(q) : Promise.resolve([]),
                _fetchDbAutocomplete(q, { fields: 'address', type: 'all' }),
            ]);
            const merged = [];
            const seen = new Set();
            googleRes.forEach(p => {
                const key = (p.description || '').toLowerCase();
                if (key && !seen.has(key)) { seen.add(key); merged.push({ description: p.description, source: 'google' }); }
            });
            dbRes.forEach(s => {
                const key = (s.value || '').toLowerCase();
                if (key && !seen.has(key)) { seen.add(key); merged.push({ description: s.value, source: 'db' }); }
            });
            currentPredictions = merged;
            selectedIdx = -1;
            renderDropdown();
        }, 200);
    });

    input.addEventListener('keydown', (e) => {
        if (dropdown.style.display !== 'block') return;
        if (e.key === 'ArrowDown') {
            e.preventDefault();
            selectedIdx = Math.min(selectedIdx + 1, currentPredictions.length - 1);
            renderDropdown();
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            selectedIdx = Math.max(selectedIdx - 1, 0);
            renderDropdown();
        } else if (e.key === 'Enter') {
            if (selectedIdx >= 0 && currentPredictions[selectedIdx]) {
                e.preventDefault();
                input.value = currentPredictions[selectedIdx].description;
                input.dispatchEvent(new Event('change', { bubbles: true }));
                hideDropdown();
            }
        } else if (e.key === 'Escape') {
            hideDropdown();
        }
    });

    input.addEventListener('blur', () => setTimeout(hideDropdown, 150));
    window.addEventListener('scroll', () => { if (dropdown.style.display === 'block') positionDropdown(); }, true);
    window.addEventListener('resize', () => { if (dropdown.style.display === 'block') positionDropdown(); });
}

// ===== DB-backed search autocomplete (for Database topbar search) =====
function initDbSearchAutocomplete(input) {
    if (!input || input._dbSearchInitialized) return;
    input._dbSearchInitialized = true;
    input.setAttribute('autocomplete', 'off');

    const dropdown = document.createElement('div');
    dropdown.className = 'hc-autocomplete-dropdown';
    dropdown.style.cssText = 'position: absolute; z-index: 10000; background: #fff; border: 1px solid #e5e7eb; border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.12); max-height: 280px; overflow-y: auto; display: none; min-width: 240px;';
    document.body.appendChild(dropdown);

    let debounceTimer = null;
    let currentItems = [];
    let selectedIdx = -1;

    const positionDropdown = () => {
        const rect = input.getBoundingClientRect();
        dropdown.style.left = (rect.left + window.scrollX) + 'px';
        dropdown.style.top = (rect.bottom + window.scrollY + 2) + 'px';
        dropdown.style.width = Math.max(rect.width, 280) + 'px';
    };
    const hideDropdown = () => { dropdown.style.display = 'none'; selectedIdx = -1; };

    const fieldLabel = (f) => ({
        address: 'Address', buyer: 'Buyer', seller: 'Seller',
        tenant_name: 'Tenant', city: 'City', zip_code: 'Zip',
    }[f] || f);

    const render = () => {
        if (!currentItems.length) { hideDropdown(); return; }
        dropdown.innerHTML = currentItems.map((s, i) => {
            const highlighted = i === selectedIdx ? 'background:#FFF3DC;' : '';
            return `
                <div class="hc-ac-item" data-idx="${i}"
                     style="padding: 8px 12px; font-size: 13px; cursor: pointer; border-bottom: 1px solid #f3f4f6; display:flex; justify-content:space-between; align-items:center; gap:8px; ${highlighted}">
                    <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${(s.value || '').toString().replace(/</g, '&lt;')}</span>
                    <span style="font-size:10px;font-weight:600;color:#8a7a5c;text-transform:uppercase;letter-spacing:0.4px;flex-shrink:0;">${fieldLabel(s.field)}</span>
                </div>`;
        }).join('');
        positionDropdown();
        dropdown.style.display = 'block';
        dropdown.querySelectorAll('.hc-ac-item').forEach(el => {
            el.addEventListener('mousedown', (e) => {
                e.preventDefault();
                const idx = parseInt(el.dataset.idx, 10);
                const pick = currentItems[idx];
                if (pick) {
                    input.value = pick.value;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('keyup', { bubbles: true }));
                }
                hideDropdown();
            });
        });
    };

    input.addEventListener('input', () => {
        const q = input.value.trim();
        clearTimeout(debounceTimer);
        if (q.length < 2) { hideDropdown(); return; }
        debounceTimer = setTimeout(async () => {
            const compType = input.dataset.compType || 'all';
            const fields = input.dataset.fields || 'address,buyer,seller,tenant_name';
            currentItems = await _fetchDbAutocomplete(q, { type: compType, fields: fields });
            selectedIdx = -1;
            render();
        }, 180);
    });

    input.addEventListener('keydown', (e) => {
        if (dropdown.style.display !== 'block') return;
        if (e.key === 'ArrowDown') { e.preventDefault(); selectedIdx = Math.min(selectedIdx + 1, currentItems.length - 1); render(); }
        else if (e.key === 'ArrowUp') { e.preventDefault(); selectedIdx = Math.max(selectedIdx - 1, 0); render(); }
        else if (e.key === 'Enter') {
            if (selectedIdx >= 0 && currentItems[selectedIdx]) {
                e.preventDefault();
                input.value = currentItems[selectedIdx].value;
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('keyup', { bubbles: true }));
                hideDropdown();
            }
        } else if (e.key === 'Escape') { hideDropdown(); }
    });

    input.addEventListener('blur', () => setTimeout(hideDropdown, 150));
    window.addEventListener('scroll', () => { if (dropdown.style.display === 'block') positionDropdown(); }, true);
    window.addEventListener('resize', () => { if (dropdown.style.display === 'block') positionDropdown(); });
}

// Auto-init on DOMContentLoaded and after HTMX swaps
function _initAllAutocompletes(root) {
    (root || document).querySelectorAll('input[data-address-autocomplete]').forEach(initAddressAutocomplete);
    (root || document).querySelectorAll('input[data-db-search-autocomplete]').forEach(initDbSearchAutocomplete);
}
document.addEventListener('DOMContentLoaded', () => _initAllAutocompletes());
document.addEventListener('htmx:afterSwap', (e) => { if (e.detail.target) _initAllAutocompletes(e.detail.target); });

// ===== Type Toggle Helper =====
function switchType(type) {
    const params = new URLSearchParams(window.location.search);
    params.set('type', type);
    window.location.href = '/database?' + params.toString();
}
