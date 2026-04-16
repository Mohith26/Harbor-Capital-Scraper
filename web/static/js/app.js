/* Harbor Capital Comp Database — Core JS */

// ===== HTMX + Alpine.js Integration =====
document.addEventListener('htmx:afterSwap', function(e) {
    if (window.Alpine) {
        Alpine.initTree(e.detail.target);
    }
});

// ===== AG Grid Helpers =====
let _gridApi = null;

function initGrid(containerId, columnDefs, rowData, options) {
    const container = document.getElementById(containerId);
    if (!container) return null;

    const defaultOptions = {
        columnDefs: columnDefs,
        rowData: rowData,
        defaultColDef: {
            sortable: true,
            filter: true,
            resizable: true,
            minWidth: 100,
        },
        rowSelection: 'multiple',
        animateRows: true,
        pagination: true,
        paginationPageSize: 50,
        domLayout: 'autoHeight',
        ...options,
    };

    if (_gridApi) {
        _gridApi.destroy();
    }
    _gridApi = agGrid.createGrid(container, defaultOptions);
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
    const map = L.map(divId).setView(center || [29.76, -95.37], zoom || 10);
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

// ===== Type Toggle Helper =====
function switchType(type) {
    const params = new URLSearchParams(window.location.search);
    params.set('type', type);
    const qs = params.toString();
    htmx.ajax('GET', '/database/table?' + qs, { target: '#table-data', swap: 'innerHTML' });
    htmx.ajax('GET', '/database/metrics?' + qs, { target: '#metrics-container', swap: 'innerHTML' });
    // Update URL without reload
    history.replaceState(null, '', '/database?' + qs);
}
