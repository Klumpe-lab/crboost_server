// Global Mol* imports
const { StateTransforms, Volume, Color } = molstar;

let viewer = null;

window.onload = async function() {
    const viewerElement = document.getElementById('app');

    viewer = await molstar.Viewer.create(viewerElement, {
        layoutIsExpanded: false,
        layoutShowControls: false,
        layoutShowRemoteState: false,
        layoutShowSequence: true,
        layoutShowLog: false,
        viewportShowOnScreenControls: true,
        viewportShowSelectionMode: true,
        viewportShowAnimation: true,
    });
    console.log('Mol* Viewer initialized');

    document.getElementById('btn-load-pdb').addEventListener('click', handleLoadPdb);
    document.getElementById('btn-load-emdb').addEventListener('click', handleLoadEmdb);
    document.getElementById('btn-clear').addEventListener('click', () => viewer.plugin.clear());
};

async function handleLoadPdb() {
    const pdbId = document.getElementById('pdb-input').value.trim().toUpperCase();
    if (!pdbId) return alert('Please enter a PDB ID');

    // POINT TO LOCAL PROXY
    // The browser asks localhost:8000, Python handles the rest.
    const url = `/api/pdb/${pdbId}`;

    try {
        console.log(`Requesting PDB from proxy: ${url}...`);
        
        // Use the high-level loader. 
        // Note: We use 'mmcif' format because .bcif is technically a variant of mmcif
        // and we pass true for isBinary.
        await viewer.loadStructureFromUrl(url, 'mmcif', true);
        
        console.log(`Success: Loaded ${pdbId}`);
    } catch (e) {
        console.error("PDB Load Failed:", e);
        alert(`Failed to load PDB ${pdbId}. Check server terminal for errors.`);
    }
}

async function handleLoadEmdb() {
    const emdbId = document.getElementById('emdb-input').value.trim();
    if (!emdbId) return alert('Please enter an EMDB ID');

    // POINT TO LOCAL PROXY
    const url = `/api/emdb/${emdbId}`;

    try {
        const plugin = viewer.plugin;
        console.log(`Requesting EMDB from proxy: ${url}...`);

        // 1. Download
        const data = await plugin.build().toRoot()
            .apply(StateTransforms.Data.Download, { url, isBinary: true, label: `EMD-${emdbId}` }, { state: { isGhost: true } })
            .apply(StateTransforms.Data.DeflateData) 
            .commit();

        // 2. Parse
        const parsed = await plugin.dataFormats.get('ccp4').parse(plugin, data, { entryId: `EMD-${emdbId}` });
        const volume = parsed.volume || parsed.volumes[0];
        
        // 3. Represent
        const volumeParams = molstar.createVolumeRepresentationParams(plugin, volume.data, {
            type: 'isosurface',
            typeParams: { 
                alpha: 0.8, 
                isoValue: Volume.adjustedIsoValue(volume.data, 1.5, 'relative') 
            },
            color: 'uniform',
            colorParams: { value: Color(0x33BB33) }
        });

        await plugin.build()
            .to(volume)
            .apply(StateTransforms.Representation.VolumeRepresentation3D, volumeParams)
            .commit();

        plugin.managers.camera.reset();
        console.log(`Success: Loaded EMD-${emdbId}`);

    } catch (e) {
        console.error("EMDB Load Failed:", e);
        alert(`Failed to load EMDB ${emdbId}. Check server terminal for errors.`);
    }
}