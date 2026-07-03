const express = require('express');
const osc = require('osc');
const path = require('path');

const app = express();
const PORT = 3000;

// Configuration RME TotalMix
const RME_IP = '127.0.0.1';
const RME_IN_PORT = 7001;  // Port sur lequel TotalMix ÉCOUTE
const RME_OUT_PORT = 9001; // Port sur lequel TotalMix ENVOIE

// Variables d'état
let currentVolume = 0.5;
let activeBus = 'output'; // Par défaut, on cible les sorties
let connectedClients = []; // Pour le temps réel (SSE)

// --- 1. CONFIGURATION DU PORT OSC (via la librairie 'osc') ---
const udpPort = new osc.UDPPort({
    localAddress: "0.0.0.0",
    localPort: RME_OUT_PORT,
    remoteAddress: RME_IP,
    remotePort: RME_IN_PORT,
    metadata: true // Permet de structurer proprement les types de données (f, s, i...)
});

// Réception des messages de TotalMix
udpPort.on("message", (oscMsg) => {
    const address = oscMsg.address;
    
    // Protection si le message n'a pas d'arguments
    if (!oscMsg.args || oscMsg.args.length === 0) return;
    const value = oscMsg.args[0].value;

    //console.log(`📩 OSC Reçu : ${address} => ${value}`); 

    // Détection du bus actif (si TotalMix l'envoie lors d'un clic dans l'interface)
    // Note : TotalMix envoie souvent 1.0 (float) quand un bus est activé
    if (address === '/1/busInput' && parseFloat(value) === 1) activeBus = 'input';
    if (address === '/1/busPlayback' && parseFloat(value) === 1) activeBus = 'playback';
    if (address === '/1/busOutput' && parseFloat(value) === 1) activeBus = 'output';

    // Mise à jour du volume (On écoute /1/volume1)
    // Optionnel : Tu peux décider de filtrer par 'activeBus === 'output'' si tu es sûr que TotalMix te renvoie le bon bus.
    if (address === '/1/volume1') {
        const numericVolume = parseFloat(value);
        if (currentVolume !== numericVolume) {
            currentVolume = numericVolume;
            
            // Pousse la nouvelle valeur vers tous les clients web connectés (SSE)
            connectedClients.forEach(client => {
                client.write(`data: ${JSON.stringify({ volume: currentVolume, bus: activeBus })}\n\n`);
            });
        }
    }
});

udpPort.on("error", (error) => {
    console.warn("⚠️ Erreur OSC capturée (paquet malformé ignoré) :", error.message);
});

// Demande à TotalMix de se caler sur le bus Output et de renvoyer son état actuel.
// Utilisé au démarrage ET à chaque nouvelle connexion web pour rafraîchir la valeur.
let lastStateRequest = 0;
function requestTotalmixState() {
    // Debounce : évite de spammer TotalMix si plusieurs clients se connectent d'un coup
    const now = Date.now();
    if (now - lastStateRequest < 500) return;
    lastStateRequest = now;

    // 1. Force TotalMix à se mettre sur le bus des Sorties (Hardware Outputs).
    //    Ce changement de page suffit généralement à faire re-émettre les faders visibles.
    udpPort.send({
        address: '/1/busOutput',
        args: [{ type: 'f', value: 1.0 }]
    });

    // 2. Demande un dump complet de l'état actuel de la table.
    //    Note : si /setSendState n'est pas reconnu par TotalMix, le /1/busOutput ci-dessus
    //    reste le mécanisme fiable de rafraîchissement.
    udpPort.send({
        address: '/setSendState',
        args: [{ type: 'f', value: 1.0 }]
    });
}

// Séquence d'initialisation dès que le port réseau est ouvert
udpPort.on("ready", () => {
    console.log(`📡 Serveur OSC en écoute des retours sur le port ${RME_OUT_PORT}`);

    // Laisse 500ms à la connexion pour se stabiliser, puis configure TotalMix
    setTimeout(() => {
        console.log("🔄 Alignement initial avec TotalMix...");
        requestTotalmixState();
    }, 500);

    // Entretien de la connexion (Le Ping indispensable pour RME)
    setInterval(() => {
        udpPort.send({
            address: '/1/ping',
            args: [{ type: 'f', value: 1.0 }]
        });
    }, 2000);
});

// Ouverture officielle du canal UDP
udpPort.open();


// --- 2. LE SERVEUR WEB (Express) ---
app.use(express.static(path.join(__dirname, 'public')));

// Connexion permanente (Server-Sent Events) pour le temps réel vers le navigateur/téléphone
app.get('/api/stream', (req, res) => {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
    });

    connectedClients.push(res);
    // Envoi immédiat du dernier état connu (peut être obsolète juste après le démarrage)
    res.write(`data: ${JSON.stringify({ volume: currentVolume, bus: activeBus })}\n\n`);

    // Re-demande l'état réel à TotalMix : la vraie valeur arrivera via le handler "message"
    // et sera poussée à ce client (et aux autres) en <1s, corrigeant une valeur obsolète.
    requestTotalmixState();

    req.on('close', () => {
        connectedClients = connectedClients.filter(client => client !== res);
    });
});

// Modification du volume depuis le téléphone/page web
app.get('/api/volume', (req, res) => {
    const volumeValue = parseFloat(req.query.val);

    if (isNaN(volumeValue) || volumeValue < 0.0 || volumeValue > 1.0) {
        return res.status(400).send('Valeur invalide (doit être entre 0.0 et 1.0)');
    }

    currentVolume = volumeValue;

    // 1. On s'assure d'abord que TotalMix cible le bon bus (Output) avant d'agir
    udpPort.send({
        address: '/1/busOutput',
        args: [{ type: 'f', value: 1.0 }]
    });

    // 2. On envoie immédiatement le changement de fader sur la piste 1
    setTimeout(() => {
        udpPort.send({
            address: '/1/volume1',
            args: [{ type: 'f', value: volumeValue }]
        });
        res.sendStatus(200);
    }, 5); // Léger délai de 5ms pour s'assurer que le changement de bus est assimilé par RME
});

// Lancement du serveur Web
app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 Serveur RME Remote démarré !`);
    console.log(`👉 http://localhost:${PORT}\n`);
});
