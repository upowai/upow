# Setting Up NGINX for uPow Node with Custom Domain

This guide will help you configure NGINX to serve your uPow node under a custom domain. Follow these steps to allow users to access your uPow node using a more friendly and professional URL.

## Prerequisites

- A server running Ubuntu.
- A domain name.
- Access to your domain's DNS settings.

## Step 1: Install NGINX

Update your package lists and install NGINX:

```bash
sudo apt-get update
sudo apt-get install nginx
```

## Step 2: Allow HTTP Traffic

Allow HTTP traffic on your firewall:

```bash
sudo ufw allow 'Nginx HTTP'
```

Check the status of NGINX:

```bash
systemctl status nginx
```

## Step 3: Configure NGINX

Edit the NGINX default site configuration:

```bash
sudo nano /etc/nginx/sites-available/default
```

Delete the existing content by pressing `Ctrl + K` repeatedly until all content is removed.

Add the following configuration. Replace `api.upow.ai` with your domain:

```nginx
server {
    listen 80;
    server_name api.upow.ai;

    location / {
        proxy_pass http://localhost:3006;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Press `Ctrl + X`, then press `Y` and `Enter` to save and exit.

## Step 4: DNS Configuration

Go to your domain registrar's DNS settings page and add an A Record:

- **Host**: `api` (or your preferred subdomain)
- **Value/IP Address**: The IP address of your server

This directs traffic for `api.yourdomain.com` to your server's IP.

## Step 5: Test NGINX Configuration

Test your NGINX configuration for syntax errors:

```bash
sudo nginx -t
```

You should see a message indicating success.

## Step 6: Adjust Firewall Settings

Adjust your firewall settings to allow full NGINX traffic and remove the redundant HTTP rule:

```bash
sudo ufw allow 'Nginx Full'
sudo ufw delete allow 'Nginx HTTP'
```

## Step 7: Reload NGINX

Finally, apply the changes by reloading NGINX:

```bash
sudo systemctl reload nginx
```

## Optional: Ensure NGINX Starts on Boot

To ensure NGINX starts automatically after a reboot, enable it:

```bash
sudo systemctl enable nginx
```

## Conclusion

Your uPow node is now accessible through your custom domain. This setup not only makes your node more accessible but also enhances its professional appearance.

Remember, any changes to your NGINX configuration should be tested with `sudo nginx -t` before applying them with a reload or restart of the NGINX service.
