#!/bin/bash
set -e

mkdir -p certs
[[ ! -f certs/cert.pem ]] && {
    openssl req -x509 -newkey rsa:2048 -keyout certs/key.pem -out certs/cert.pem -days 365 -nodes -subj "/CN=localhost" &>/dev/null
    chmod 644 certs/cert.pem
    chmod 644 certs/key.pem
    sudo chown 1000:1000 certs/cert.pem certs/key.pem 2>/dev/null || true
}

docker compose up -d


echo "Waiting for Gitea to be ready"
for i in {1..120}; do
    if curl -sf -k https://localhost:3443/api/v1/version &>/dev/null; then
        echo "Gitea is ready after $i attempts"
        break
    fi
    if [ $((i % 20)) -eq 0 ]; then
        docker ps --filter "name=gitea" --format "table {{.Names}}\t{{.Status}}"
    fi
    sleep 3
done

curl -sf -k https://localhost:3443/api/v1/version || {
    echo "Gitea not responding after 6 minutes"
    echo "Container logs:"
    docker logs gitea-test --tail 50
    exit 1
}


CONTAINER=$(docker ps --filter "name=gitea" --format "{{.ID}}" | head -n1)
[[ -z "$CONTAINER" ]] && { echo "No Gitea container found!"; docker ps; exit 1; }


docker exec -u git $CONTAINER gitea admin user create --username gitea_admin --password password123 --email gitea_admin@example.com --admin 2>/dev/null || true
sleep 5

echo "Generating token..."
GITEA_PAT=$(docker exec -u git $CONTAINER gitea admin user generate-access-token --username gitea_admin --scopes "all" 2>/dev/null | grep -o '[a-f0-9]\{40\}' | head -n1)

[[ -z "$GITEA_PAT" ]] && { echo "Failed to generate PAT"; exit 1; }
echo "Generated PAT: $GITEA_PAT"

# here we test if the token we generated works
curl -k -sf -H "Authorization: token $GITEA_PAT" "https://localhost:3443/api/v1/user" || { echo "Token validation failed"; exit 1; }

# here we create a Repo
curl -k -sf -X POST "https://localhost:3443/api/v1/user/repos" -H "Authorization: token $GITEA_PAT" -H "Content-Type: application/json" -d '{"name":"kestra-test"}' &>/dev/null
README_B64=$(echo -n "# Kestra Test Repository" | base64 | tr -d '\n')
curl -k -sf -X POST "https://localhost:3443/api/v1/repos/gitea_admin/kestra-test/contents/README.md" -H "Authorization: token $GITEA_PAT" -H "Content-Type: application/json" -d "{\"content\":\"$README_B64\",\"message\":\"Initial commit\"}" &>/dev/null || true

mkdir -p src/test/resources
cat > src/test/resources/application-test.yml <<EOF
kestra:
  git:
    pat: \${GH_PERSONAL_TOKEN:}
  gitea:
    pat: $GITEA_PAT
EOF

echo "Setup complete - PAT: $GITEA_PAT"
echo "Repository URL: https://localhost:3443/gitea_admin/kestra-test.git"

# CLEANUP
rm -rf certs
