/**
 * Cloudflare Worker: GitHub OAuth proxy for Decap CMS
 *
 * Environment variables (set in Cloudflare dashboard → Worker → Settings → Variables):
 *   GITHUB_CLIENT_ID     — from your GitHub OAuth App
 *   GITHUB_CLIENT_SECRET — from your GitHub OAuth App
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/auth') {
      return handleAuth(env);
    }

    if (url.pathname === '/callback') {
      return handleCallback(url, env);
    }

    return new Response('Not found', { status: 404 });
  },
};

function handleAuth(env) {
  const params = new URLSearchParams({
    client_id: env.GITHUB_CLIENT_ID,
    scope: 'repo,user',
  });
  return Response.redirect(
    `https://github.com/login/oauth/authorize?${params}`,
    302
  );
}

async function handleCallback(url, env) {
  const code = url.searchParams.get('code');
  if (!code) {
    return sendMessage('error', 'No authorization code received from GitHub.');
  }

  const tokenRes = await fetch('https://github.com/login/oauth/access_token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify({
      client_id: env.GITHUB_CLIENT_ID,
      client_secret: env.GITHUB_CLIENT_SECRET,
      code,
    }),
  });

  const data = await tokenRes.json();

  if (data.error || !data.access_token) {
    return sendMessage(
      'error',
      data.error_description || 'Failed to get access token from GitHub.'
    );
  }

  return sendMessage(
    'success',
    JSON.stringify({ token: data.access_token, provider: 'github' })
  );
}

function sendMessage(status, content) {
  const message = `authorization:github:${status}:${content}`;
  const html = `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Authenticating...</title></head>
<body>
<p>Authenticating, please wait...</p>
<script>
(function () {
  var msg = ${JSON.stringify(message)};
  function send() {
    if (window.opener) {
      window.opener.postMessage(msg, '*');
      setTimeout(function () { window.close(); }, 500);
    }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', send);
  } else {
    send();
  }
})();
<\/script>
</body>
</html>`;

  return new Response(html, {
    headers: { 'Content-Type': 'text/html; charset=utf-8' },
  });
}
