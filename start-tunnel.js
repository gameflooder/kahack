const { spawn } = require('child_process');
const path = require('path');

const tunnel = spawn('C:\\Users\\sebmo\\Downloads\\cloudflared.exe', ['tunnel', '--url', 'http://localhost:9235'], {
    stdio: ['inherit', 'pipe', 'pipe']
});

let urlFound = false;

function checkForUrl(data) {
    const text = data.toString();
    const match = text.match(/https:\/\/[a-z0-9-]+\.trycloudflare\.com/i);
    if (match && !urlFound) {
        urlFound = true;
        const tunnelUrl = match[0];
        console.log(`\n✓ Tunnel URL: ${tunnelUrl}`);
        
        const updater = spawn('node', [path.join(__dirname, 'updateBackendUrl.js'), tunnelUrl], {
            stdio: 'inherit',
            env: process.env  // Pass all environment variables including GITHUB_TOKEN
        });
        
        updater.on('close', (code) => {
            if (code === 0) {
                console.log('✓ GitHub backend.json updated!\n');
            } else {
                console.error('✗ Failed to update GitHub backend.json');
            }
        });
    }
    process.stdout.write(data);
}

tunnel.stdout.on('data', checkForUrl);
tunnel.stderr.on('data', checkForUrl);

tunnel.on('close', (code) => {
    console.log(`Cloudflared exited with code ${code}`);
    process.exit(code);
});

process.on('SIGINT', () => {
    tunnel.kill();
    process.exit();
});
