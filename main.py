import asyncio
import json
import base64
import email
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Union
import os
from datetime import datetime
import logging

# MCP and HTTP imports
from mcp.server import Server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel
)
import mcp.server.stdio
import mcp.types

# Google API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Web framework for OAuth and SSE
from flask import Flask, request, jsonify, Response, redirect, session
from flask_cors import CORS
import threading
import queue
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GmailMCPServer:
    def __init__(self):
        self.server = Server("gmail-mcp")
        self.flask_app = Flask(__name__)
        self.flask_app.secret_key = os.getenv('FLASK_SECRET_KEY', 'your-secret-key-change-this')
        CORS(self.flask_app)
        
        # OAuth configuration
        self.CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
        self.CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
        self.REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:5000/oauth/callback')
        
        # Gmail scopes
        self.SCOPES = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/gmail.modify'
        ]
        
        self.credentials = None
        self.gmail_service = None
        
        # SSE clients
        self.sse_clients = set()
        self.message_queue = queue.Queue()
        
        self.setup_mcp_handlers()
        self.setup_flask_routes()
    
    def setup_mcp_handlers(self):
        """Setup MCP server handlers"""
        
        @self.server.list_resources()
        async def handle_list_resources() -> List[Resource]:
            """List available Gmail resources"""
            if not self.gmail_service:
                return []
            
            resources = []
            try:
                # Get recent emails
                results = self.gmail_service.users().messages().list(
                    userId='me', maxResults=10
                ).execute()
                
                messages = results.get('messages', [])
                
                for msg in messages:
                    msg_detail = self.gmail_service.users().messages().get(
                        userId='me', id=msg['id']
                    ).execute()
                    
                    subject = "No Subject"
                    for header in msg_detail.get('payload', {}).get('headers', []):
                        if header['name'] == 'Subject':
                            subject = header['value']
                            break
                    
                    resources.append(Resource(
                        uri=f"gmail://message/{msg['id']}",
                        name=f"Email: {subject}",
                        description=f"Gmail message with ID: {msg['id']}",
                        mimeType="text/plain"
                    ))
                    
            except Exception as e:
                logger.error(f"Error listing resources: {e}")
            
            return resources
        
        @self.server.read_resource()
        async def handle_read_resource(uri: str) -> str:
            """Read a specific Gmail resource"""
            if not self.gmail_service:
                raise ValueError("Gmail service not authenticated")
            
            if not uri.startswith("gmail://message/"):
                raise ValueError("Invalid Gmail URI")
            
            message_id = uri.replace("gmail://message/", "")
            
            try:
                message = self.gmail_service.users().messages().get(
                    userId='me', id=message_id, format='full'
                ).execute()
                
                return self.extract_email_content(message)
                
            except HttpError as e:
                raise ValueError(f"Error reading email: {e}")
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[Tool]:
            """List available Gmail tools"""
            return [
                Tool(
                    name="search_emails",
                    description="Search for emails in Gmail",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Gmail search query (e.g., 'from:sender@email.com', 'subject:important')"
                            },
                            "max_results": {
                                "type": "integer",
                                "description": "Maximum number of results to return",
                                "default": 10
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="get_email_attachments",
                    description="Get attachments from a specific email",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "message_id": {
                                "type": "string",
                                "description": "Gmail message ID"
                            }
                        },
                        "required": ["message_id"]
                    }
                ),
                Tool(
                    name="get_email_thread",
                    description="Get all emails in a thread",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "thread_id": {
                                "type": "string",
                                "description": "Gmail thread ID"
                            }
                        },
                        "required": ["thread_id"]
                    }
                )
            ]
        
        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            """Handle tool calls"""
            if not self.gmail_service:
                return [TextContent(type="text", text="Error: Gmail service not authenticated")]
            
            try:
                if name == "search_emails":
                    return await self.search_emails(arguments.get("query", ""), arguments.get("max_results", 10))
                elif name == "get_email_attachments":
                    return await self.get_email_attachments(arguments.get("message_id"))
                elif name == "get_email_thread":
                    return await self.get_email_thread(arguments.get("thread_id"))
                else:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Error executing tool {name}: {str(e)}")]
    
    def setup_flask_routes(self):
        """Setup Flask routes for OAuth and SSE"""
        
        @self.flask_app.route('/oauth/login')
        def oauth_login():
            """Initiate OAuth flow"""
            flow = Flow.from_client_config(
                {
                    "web": {
                        "client_id": self.CLIENT_ID,
                        "client_secret": self.CLIENT_SECRET,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": [self.REDIRECT_URI]
                    }
                },
                scopes=self.SCOPES
            )
            flow.redirect_uri = self.REDIRECT_URI
            
            authorization_url, state = flow.authorization_url(
                access_type='offline',
                include_granted_scopes='true'
            )
            
            session['state'] = state
            return redirect(authorization_url)
        
        @self.flask_app.route('/oauth/callback')
        def oauth_callback():
            """Handle OAuth callback"""
            state = session.get('state')
            
            flow = Flow.from_client_config(
                {
                    "web": {
                        "client_id": self.CLIENT_ID,
                        "client_secret": self.CLIENT_SECRET,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": [self.REDIRECT_URI]
                    }
                },
                scopes=self.SCOPES,
                state=state
            )
            flow.redirect_uri = self.REDIRECT_URI
            
            authorization_response = request.url
            flow.fetch_token(authorization_response=authorization_response)
            
            self.credentials = flow.credentials
            self.gmail_service = build('gmail', 'v1', credentials=self.credentials)
            
            # Broadcast authentication success via SSE
            self.broadcast_sse_message({
                'type': 'auth_success',
                'message': 'Gmail authentication successful'
            })
            
            return jsonify({'status': 'success', 'message': 'Authentication successful'})
        
        @self.flask_app.route('/sse')
        def sse():
            """Server-Sent Events endpoint"""
            def event_stream():
                client_queue = queue.Queue()
                self.sse_clients.add(client_queue)
                
                try:
                    while True:
                        try:
                            # Wait for message with timeout
                            message = client_queue.get(timeout=30)
                            yield f"data: {json.dumps(message)}\n\n"
                        except queue.Empty:
                            # Send keepalive
                            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                except GeneratorExit:
                    self.sse_clients.discard(client_queue)
            
            return Response(
                event_stream(),
                mimetype='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Cache-Control'
                }
            )
        
        @self.flask_app.route('/status')
        def status():
            """Check authentication status"""
            return jsonify({
                'authenticated': self.gmail_service is not None,
                'credentials_valid': self.credentials is not None and self.credentials.valid
            })
        
        @self.flask_app.route('/health')
        def health():
            """Health check endpoint"""
            return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})
    
    def broadcast_sse_message(self, message: Dict[str, Any]):
        """Broadcast message to all SSE clients"""
        disconnected_clients = set()
        
        for client_queue in self.sse_clients:
            try:
                client_queue.put_nowait(message)
            except queue.Full:
                disconnected_clients.add(client_queue)
        
        # Remove disconnected clients
        self.sse_clients -= disconnected_clients
    
    def extract_email_content(self, message: Dict[str, Any]) -> str:
        """Extract readable content from Gmail message"""
        content_parts = []
        
        # Extract headers
        headers = message.get('payload', {}).get('headers', [])
        header_info = {}
        for header in headers:
            if header['name'] in ['From', 'To', 'Subject', 'Date']:
                header_info[header['name']] = header['value']
        
        content_parts.append("=== EMAIL HEADERS ===")
        for key, value in header_info.items():
            content_parts.append(f"{key}: {value}")
        content_parts.append("\n=== EMAIL BODY ===")
        
        # Extract body
        payload = message.get('payload', {})
        body_text = self.extract_text_from_payload(payload)
        content_parts.append(body_text)
        
        return "\n".join(content_parts)
    
    def extract_text_from_payload(self, payload: Dict[str, Any]) -> str:
        """Extract text content from email payload"""
        if 'parts' in payload:
            text_content = []
            for part in payload['parts']:
                if part.get('mimeType') == 'text/plain':
                    data = part.get('body', {}).get('data', '')
                    if data:
                        decoded = base64.urlsafe_b64decode(data).decode('utf-8')
                        text_content.append(decoded)
                elif part.get('mimeType') == 'text/html' and not text_content:
                    # Fallback to HTML if no plain text
                    data = part.get('body', {}).get('data', '')
                    if data:
                        decoded = base64.urlsafe_b64decode(data).decode('utf-8')
                        text_content.append(f"[HTML Content]: {decoded[:500]}...")
            return "\n".join(text_content)
        else:
            # Single part message
            if payload.get('mimeType') == 'text/plain':
                data = payload.get('body', {}).get('data', '')
                if data:
                    return base64.urlsafe_b64decode(data).decode('utf-8')
        
        return "No readable content found"
    
    async def search_emails(self, query: str, max_results: int = 10) -> List[TextContent]:
        """Search for emails"""
        try:
            results = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = results.get('messages', [])
            
            if not messages:
                return [TextContent(type="text", text="No emails found matching the query.")]
            
            email_summaries = []
            for msg in messages[:max_results]:
                msg_detail = self.gmail_service.users().messages().get(
                    userId='me', id=msg['id']
                ).execute()
                
                headers = msg_detail.get('payload', {}).get('headers', [])
                subject = "No Subject"
                sender = "Unknown Sender"
                date = "Unknown Date"
                
                for header in headers:
                    if header['name'] == 'Subject':
                        subject = header['value']
                    elif header['name'] == 'From':
                        sender = header['value']
                    elif header['name'] == 'Date':
                        date = header['value']
                
                email_summaries.append(f"ID: {msg['id']}\nFrom: {sender}\nSubject: {subject}\nDate: {date}\n---")
            
            return [TextContent(type="text", text="\n".join(email_summaries))]
            
        except Exception as e:
            return [TextContent(type="text", text=f"Error searching emails: {str(e)}")]
    
    async def get_email_attachments(self, message_id: str) -> List[TextContent]:
        """Get attachments from an email"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id
            ).execute()
            
            attachments = []
            payload = message.get('payload', {})
            
            def extract_attachments(part):
                if part.get('filename'):
                    attachments.append({
                        'filename': part['filename'],
                        'mimeType': part.get('mimeType', 'unknown'),
                        'size': part.get('body', {}).get('size', 0),
                        'attachmentId': part.get('body', {}).get('attachmentId')
                    })
                
                if 'parts' in part:
                    for subpart in part['parts']:
                        extract_attachments(subpart)
            
            extract_attachments(payload)
            
            if not attachments:
                return [TextContent(type="text", text="No attachments found in this email.")]
            
            attachment_info = []
            for att in attachments:
                attachment_info.append(
                    f"Filename: {att['filename']}\n"
                    f"Type: {att['mimeType']}\n"
                    f"Size: {att['size']} bytes\n"
                    f"Attachment ID: {att['attachmentId']}\n---"
                )
            
            return [TextContent(type="text", text="\n".join(attachment_info))]
            
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting attachments: {str(e)}")]
    
    async def get_email_thread(self, thread_id: str) -> List[TextContent]:
        """Get all emails in a thread"""
        try:
            thread = self.gmail_service.users().threads().get(
                userId='me', id=thread_id
            ).execute()
            
            messages = thread.get('messages', [])
            thread_content = []
            
            for i, message in enumerate(messages):
                thread_content.append(f"=== MESSAGE {i+1} ===")
                content = self.extract_email_content(message)
                thread_content.append(content)
                thread_content.append("\n" + "="*50 + "\n")
            
            return [TextContent(type="text", text="\n".join(thread_content))]
            
        except Exception as e:
            return [TextContent(type="text", text=f"Error getting thread: {str(e)}")]
    
    async def run_mcp_server(self):
        """Run the MCP server"""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options()
            )
    
    def run_flask_server(self):
        """Run the Flask server for OAuth and SSE"""
        port = int(os.getenv('PORT', 10000))
        self.flask_app.run(host='0.0.0.0', port=port, threaded=True)
    
    async def run(self):
        """Run both servers"""
        # Start Flask server in a separate thread
        flask_thread = threading.Thread(target=self.run_flask_server)
        flask_thread.daemon = True
        flask_thread.start()
        
        logger.info("Gmail MCP Server starting...")
        logger.info(f"OAuth login URL: {self.REDIRECT_URI.replace('/oauth/callback', '/oauth/login')}")
        logger.info(f"SSE endpoint: {self.REDIRECT_URI.replace('/oauth/callback', '/sse')}")
        
        # Run MCP server
        await self.run_mcp_server()

def main():
    """Main entry point"""
    # Validate required environment variables
    required_env_vars = ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return
    
    server = GmailMCPServer()
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")

if __name__ == "__main__":
    main()
