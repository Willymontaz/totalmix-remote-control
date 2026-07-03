// --- Pont vers CamillaDSP (WebSocket) ---
//
// CamillaDSP expose un serveur WebSocket qui accepte des commandes JSON et
// répond avec un objet dont la seule clé est le nom de la commande.
// On s'en sert ici pour :
//   - activer / désactiver les processeurs correctifs "Expander" (expansion
//     vers le haut) et "Declipper" en basculant le champ `bypassed` de leur
//     étape de pipeline (simple reconstruction du pipeline, sans recharger de
//     fichier de config) ;
//   - lire en direct s'ils *interviennent* (GetExpansionGain / GetDeclippedSamples)
//     pour afficher un voyant d'activité côté téléphone.

const WebSocket = require('ws');

const CAMILLA_HOST = process.env.CAMILLA_HOST || '127.0.0.1';
// Pas de port par défaut côté CamillaDSP ; 1234 est la convention de l'écosystème
// (pycamilladsp / la GUI). Surchargeable via la variable d'env CAMILLA_PORT.
const CAMILLA_PORT = process.env.CAMILLA_PORT || 1234;
const CAMILLA_URL = `ws://${CAMILLA_HOST}:${CAMILLA_PORT}`;
const REQUEST_TIMEOUT = 3000; // ms

// Processeurs correctifs exposés à la remote. `type` = le champ "type" du
// processeur dans la config CamillaDSP ; `gainCmd`/`countCmd` = la commande de
// télémétrie qui dit s'il intervient en ce moment.
const PROCESSORS = {
    expander: { type: 'Expander', gainCmd: 'GetExpansionGain' },
    declipper: { type: 'Declipper', countCmd: 'GetDeclippedSamples' },
};

// Un processeur est considéré "en action" (voyant allumé) dès que sa métrique
// dépasse le seuil ; on garde le voyant allumé ENGAGE_LATCH_MS après le dernier
// dépassement pour qu'un événement bref reste visible entre deux sondages.
const EXPAND_ENGAGE_DB = 0.05; // dB de gain d'expansion au-delà duquel c'est "actif"
const ENGAGE_LATCH_MS = 2500;

// État conservé entre les sondages pour lisser l'affichage du voyant.
const engage = {
    expander: { lastTs: 0, gainDb: 0 },
    declipper: { lastTs: 0, lastCount: null, count: 0 },
};

// Envoie UNE commande à CamillaDSP et résout avec le corps de la réponse.
// Une connexion courte par requête : robuste et sans état, largement suffisant
// pour un simple toggle.
function camillaRequest(command) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(CAMILLA_URL);
        let settled = false;

        const finish = (fn, arg) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            try { ws.close(); } catch (_) { /* ignore */ }
            fn(arg);
        };

        const timer = setTimeout(
            () => finish(reject, new Error('CamillaDSP timeout')),
            REQUEST_TIMEOUT
        );

        ws.on('open', () => ws.send(JSON.stringify(command)));
        ws.on('message', (data) => {
            try {
                const reply = JSON.parse(data.toString());
                const key = Object.keys(reply)[0]; // ex: "GetConfigJson", "SetConfigValue"
                finish(resolve, reply[key]);
            } catch (err) {
                finish(reject, err);
            }
        });
        ws.on('error', (err) => finish(reject, err));
    });
}

// Envoie PLUSIEURS commandes sur une seule connexion et résout avec un objet
// { nomDeCommande: corps }. Comme les réponses sont indexées par le nom de la
// commande, on peut tout dispatcher sans corréler l'ordre des réponses. On
// attend simplement autant de messages que de commandes envoyées (une commande
// non supportée répond `{"Invalid":…}` : c'est un message de plus, on ne bloque
// donc pas — la clé attendue restera juste absente).
function camillaRequestMany(commands) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(CAMILLA_URL);
        const results = {};
        let received = 0;
        let settled = false;

        const finish = (fn, arg) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            try { ws.close(); } catch (_) { /* ignore */ }
            fn(arg);
        };

        const timer = setTimeout(
            () => finish(reject, new Error('CamillaDSP timeout')),
            REQUEST_TIMEOUT
        );

        ws.on('open', () => commands.forEach((c) => ws.send(JSON.stringify(c))));
        ws.on('message', (data) => {
            try {
                const reply = JSON.parse(data.toString());
                const key = Object.keys(reply)[0];
                results[key] = reply[key];
            } catch (_) { /* on ignore un message illisible mais on le compte */ }
            received += 1;
            if (received >= commands.length) finish(resolve, results);
        });
        ws.on('error', (err) => finish(reject, err));
    });
}

// Récupère la config active de CamillaDSP (désérialisée depuis le JSON).
async function getConfig() {
    const body = await camillaRequest('GetConfigJson');
    if (!body || body.result !== 'Ok') {
        throw new Error('GetConfigJson refusé par CamillaDSP');
    }
    return JSON.parse(body.value);
}

// Localise la première étape de pipeline qui exécute un processeur du type
// demandé. Retourne { index, name, bypassed } ou null si absent.
function findProcessorStep(config, procType) {
    const pipeline = config.pipeline || [];
    const processors = config.processors || {};
    for (let i = 0; i < pipeline.length; i++) {
        const step = pipeline[i];
        if (step.type === 'Processor') {
            const proc = processors[step.name];
            if (proc && proc.type === procType) {
                return { index: i, name: step.name, bypassed: step.bypassed === true };
            }
        }
    }
    return null;
}

// Active (active=true) ou désactive (active=false) un processeur en basculant le
// champ `bypassed` de son étape de pipeline via SetConfigValue.
async function setProcessorActive(procKey, active) {
    const spec = PROCESSORS[procKey];
    if (!spec) throw new Error(`Processeur inconnu : ${procKey}`);
    const config = await getConfig();
    const step = findProcessorStep(config, spec.type);
    if (!step) {
        throw new Error(`Aucun processeur ${spec.type} dans le pipeline CamillaDSP`);
    }
    const pointer = `/pipeline/${step.index}/bypassed`;
    const body = await camillaRequest({ SetConfigValue: [pointer, !active] });
    if (!body || body.result !== 'Ok') {
        const detail = body ? JSON.stringify(body.result) : 'pas de réponse';
        throw new Error(`SetConfigValue échoué : ${detail}`);
    }
    return { available: true, found: true, active, name: step.name };
}

// État complet des deux processeurs, tel que présenté au front-end :
// pour chacun -> présent dans le pipeline ? activé (non bypassé) ? en action ?
// Un seul aller-retour WebSocket regroupe la config et la télémétrie.
async function getStatus() {
    let results;
    try {
        results = await camillaRequestMany([
            'GetConfigJson',
            PROCESSORS.expander.gainCmd,
            PROCESSORS.declipper.countCmd,
        ]);
    } catch (err) {
        return {
            available: false,
            error: err.message,
            expander: { available: false, found: false, active: false, engaging: false, name: null },
            declipper: { available: false, found: false, active: false, engaging: false, name: null },
        };
    }

    const cfgBody = results.GetConfigJson;
    if (!cfgBody || cfgBody.result !== 'Ok') {
        return {
            available: false,
            error: 'GetConfigJson refusé',
            expander: { available: false, found: false, active: false, engaging: false, name: null },
            declipper: { available: false, found: false, active: false, engaging: false, name: null },
        };
    }
    const config = JSON.parse(cfgBody.value);
    const now = Date.now();

    // --- Expander ---
    const expStep = findProcessorStep(config, PROCESSORS.expander.type);
    const gainBody = results[PROCESSORS.expander.gainCmd];
    const gainDb = gainBody && gainBody.result === 'Ok' ? gainBody.value : null;
    if (gainDb != null) {
        engage.expander.gainDb = gainDb;
        if (gainDb > EXPAND_ENGAGE_DB) engage.expander.lastTs = now;
    }
    const expander = {
        available: true,
        found: !!expStep,
        active: expStep ? !expStep.bypassed : false,
        name: expStep ? expStep.name : null,
        telemetry: gainDb != null,
        gainDb: engage.expander.gainDb,
        engaging: (now - engage.expander.lastTs) < ENGAGE_LATCH_MS,
    };

    // --- Declipper ---
    const decStep = findProcessorStep(config, PROCESSORS.declipper.type);
    const countBody = results[PROCESSORS.declipper.countCmd];
    const count = countBody && countBody.result === 'Ok' ? countBody.value : null;
    if (count != null) {
        if (engage.declipper.lastCount != null && count > engage.declipper.lastCount) {
            engage.declipper.lastTs = now;
        }
        engage.declipper.lastCount = count;
        engage.declipper.count = count;
    }
    const declipper = {
        available: true,
        found: !!decStep,
        active: decStep ? !decStep.bypassed : false,
        name: decStep ? decStep.name : null,
        telemetry: count != null,
        count: engage.declipper.count,
        engaging: (now - engage.declipper.lastTs) < ENGAGE_LATCH_MS,
    };

    return { available: true, expander, declipper };
}

module.exports = { getStatus, setProcessorActive, CAMILLA_URL };
