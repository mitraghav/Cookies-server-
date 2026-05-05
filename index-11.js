// save as server.js
// npm install express ws axios w3-fca uuid express-session multer

const fs = require('fs');
const path = require('path');
const express = require('express');
const wiegine = require('ws3-fca');
const WebSocket = require('ws');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const session = require('express-session');
const crypto = require('crypto');
const multer = require('multer');

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 21120;

// Create uploads directory if not exists
if (!fs.existsSync('./uploads')) {
    fs.mkdirSync('./uploads');
}

// Configure multer for file uploads
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, './uploads/');
    },
    filename: function (req, file, cb) {
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
        cb(null, uniqueSuffix + path.extname(file.originalname));
    }
});

const upload = multer({ 
    storage: storage,
    limits: { fileSize: 10 * 1024 * 1024 } // 10MB limit
});

// User sessions and task storage
const userSessions = new Map(); // sessionId -> userId
const userTasks = new Map(); // userId -> [task1, task2, ...]

// Persistent storage file
const STORAGE_FILE = './user_tasks.json';

// Load tasks from storage
function loadTasksFromStorage() {
    try {
        if (fs.existsSync(STORAGE_FILE)) {
            const data = fs.readFileSync(STORAGE_FILE, 'utf8');
            const saved = JSON.parse(data);
            
            for (const [userId, taskData] of Object.entries(saved)) {
                if (!userTasks.has(userId)) {
                    const user = new User(userId);
                    userTasks.set(userId, user);
                    
                    // We'll restore tasks as inactive since they can't be reconnected
                    console.log(`📂 Loaded saved tasks for user: ${userId}`);
                }
            }
            console.log(`📂 Loaded ${Object.keys(saved).length} users from storage`);
        }
    } catch (error) {
        console.error('Error loading tasks from storage:', error);
    }
}

// Save tasks to storage
function saveTasksToStorage() {
    try {
        const data = {};
        userTasks.forEach((user, userId) => {
            const tasks = user.getAllTasks();
            if (tasks.length > 0) {
                data[userId] = tasks.map(task => ({
                    taskId: task.taskId,
                    userId: task.userId,
                    stats: task.stats,
                    config: {
                        running: false, // Save as stopped
                        lastActivity: task.config.lastActivity
                    },
                    userData: task.userData,
                    createdAt: task.stats.createdAt
                }));
            }
        });
        
        fs.writeFileSync(STORAGE_FILE, JSON.stringify(data, null, 2));
        console.log(`💾 Saved ${Object.keys(data).length} users to storage`);
    } catch (error) {
        console.error('Error saving tasks to storage:', error);
    }
}

// Auto console clear setup
let consoleClearInterval;
function setupConsoleClear() {
    consoleClearInterval = setInterval(() => {
        console.clear();
        console.log(`🔄 Console cleared at: ${new Date().toLocaleTimeString()}`);
        console.log(`👥 Active users: ${userSessions.size}`);
        let totalTasks = 0;
        userTasks.forEach(tasks => totalTasks += tasks.tasks.size);
        console.log(`📊 Total tasks: ${totalTasks}`);
        console.log(`💾 Memory usage: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
        
        // Auto-save to storage
        saveTasksToStorage();
    }, 30 * 60 * 1000);
}

// User class to manage user-specific data
class User {
    constructor(userId) {
        this.userId = userId;
        this.tasks = new Map(); // taskId -> Task
        this.createdAt = Date.now();
        this.lastActivity = Date.now();
        this.sessionId = uuidv4();
    }

    addTask(taskId, task) {
        this.tasks.set(taskId, task);
        this.lastActivity = Date.now();
        saveTasksToStorage(); // Auto-save on task addition
        return task;
    }

    getTask(taskId) {
        this.lastActivity = Date.now();
        return this.tasks.get(taskId);
    }

    getAllTasks() {
        this.lastActivity = Date.now();
        return Array.from(this.tasks.values());
    }

    removeTask(taskId) {
        const task = this.tasks.get(taskId);
        if (task) {
            task.stop();
            this.tasks.delete(taskId);
        }
        this.lastActivity = Date.now();
        saveTasksToStorage(); // Auto-save on task removal
        return task;
    }

    stopAllTasks() {
        this.tasks.forEach(task => task.stop());
        this.tasks.clear();
        this.lastActivity = Date.now();
        saveTasksToStorage(); // Auto-save on stop all
    }

    healthCheck() {
        return Date.now() - this.lastActivity < 3600000; // 1 hour
    }
}

// Task class
class Task {
    constructor(taskId, userData, userId) {
        this.taskId = taskId;
        this.userId = userId;
        this.userData = userData;
        
        // Parse multiple cookies
        this.cookies = this.parseCookies(userData.cookieContent);
        this.currentCookieIndex = -1;
        
        this.config = {
            prefix: '',
            delay: userData.delay || 5,
            running: false,
            apis: [],
            repeat: true,
            lastActivity: Date.now(),
            restartCount: 0,
            maxRestarts: 1000
        };
        this.messageData = {
            threadID: userData.threadID,
            messages: [],
            currentIndex: 0,
            loopCount: 0
        };
        this.stats = {
            sent: 0,
            failed: 0,
            activeCookies: 0,
            totalCookies: this.cookies.length,
            loops: 0,
            restarts: 0,
            lastSuccess: null,
            cookieUsage: Array(this.cookies.length).fill(0),
            createdAt: new Date().toISOString(),
            owner: userId
        };
        this.logs = [];
        this.retryCount = 0;
        this.maxRetries = 50;
        this.initializeMessages(userData.messageContent, userData.hatersName, userData.lastHereName);
    }

    parseCookies(cookieContent) {
        const cookies = [];
        const lines = cookieContent.split('\n')
            .map(line => line.trim())
            .filter(line => line.length > 0);
        
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            try {
                JSON.parse(line);
                cookies.push(line);
            } catch {
                cookies.push(line);
            }
        }
        return cookies;
    }

    initializeMessages(messageContent, hatersName, lastHereName) {
        this.messageData.messages = messageContent
            .split('\n')
            .map(line => line.replace(/\r/g, '').trim())
            .filter(line => line.length > 0)
            .map(message => `${hatersName} ${message} ${lastHereName}`);
        
        this.addLog(`Loaded ${this.messageData.messages.length} formatted messages`);
        this.addLog(`Detected ${this.cookies.length} cookies in file`, 'info');
    }

    addLog(message, messageType = 'info') {
        const logEntry = {
            time: new Date().toLocaleTimeString('en-PK', { timeZone: 'Asia/Karachi' }),
            message: message,
            type: messageType
        };
        this.logs.unshift(logEntry);
        if (this.logs.length > 100) this.logs = this.logs.slice(0, 100);
        
        this.config.lastActivity = Date.now();
        broadcastToUser(this.userId, this.taskId, {
            type: 'log',
            message: message,
            messageType: messageType
        });
    }

    async start() {
        if (this.config.running) {
            this.addLog('Task is already running', 'info');
            return true;
        }

        this.config.running = true;
        this.retryCount = 0;
        
        if (this.messageData.messages.length === 0) {
            this.addLog('No messages found in the file', 'error');
            this.config.running = false;
            return false;
        }

        this.addLog(`Starting task with ${this.messageData.messages.length} messages and ${this.cookies.length} cookies`);
        return this.initializeAllBots();
    }

    initializeAllBots() {
        return new Promise((resolve) => {
            let currentIndex = 0;
            const totalCookies = this.cookies.length;
            
            const loginNextCookie = () => {
                if (currentIndex >= totalCookies) {
                    if (this.stats.activeCookies > 0) {
                        this.addLog(`✅ ${this.stats.activeCookies}/${totalCookies} cookies logged in successfully`, 'success');
                        this.startSending();
                        resolve(true);
                    } else {
                        this.addLog('❌ All cookies failed to login', 'error');
                        resolve(false);
                    }
                    return;
                }
                
                const cookieIndex = currentIndex;
                const cookieContent = this.cookies[cookieIndex];
                
                setTimeout(() => {
                    this.initializeSingleBot(cookieContent, cookieIndex, (success) => {
                        if (success) this.stats.activeCookies++;
                        currentIndex++;
                        loginNextCookie();
                    });
                }, cookieIndex * 2000);
            };
            
            loginNextCookie();
        });
    }

    initializeSingleBot(cookieContent, index, callback) {
        this.addLog(`Attempting login for Cookie ${index + 1}...`, 'info');
        
        wiegine.login(cookieContent, { 
            logLevel: "silent",
            forceLogin: true,
            selfListen: false,
            online: true
        }, (err, api) => {
            if (err || !api) {
                this.addLog(`❌ Cookie ${index + 1} login failed: ${err ? err.message : 'Unknown error'}`, 'error');
                this.config.apis[index] = null;
                callback(false);
                return;
            }

            this.config.apis[index] = api;
            this.addLog(`✅ Cookie ${index + 1} logged in successfully`, 'success');
            this.setupApiErrorHandling(api, index);
            this.getGroupInfo(api, this.messageData.threadID, index);
            callback(true);
        });
    }

    setupApiErrorHandling(api, index) {
        if (api && typeof api.listen === 'function') {
            try {
                api.listen((err, event) => {
                    if (err && this.config.running) {
                        this.config.apis[index] = null;
                        this.stats.activeCookies = this.config.apis.filter(api => api !== null).length;
                        this.addLog(`⚠️ Cookie ${index + 1} disconnected, will retry`, 'warning');
                        
                        setTimeout(() => {
                            if (this.config.running) {
                                this.addLog(`🔄 Reconnecting Cookie ${index + 1}...`, 'info');
                                this.initializeSingleBot(this.cookies[index], index, (success) => {
                                    if (success) this.stats.activeCookies++;
                                });
                            }
                        }, 30000);
                    }
                });
            } catch (e) {}
        }
    }

    getGroupInfo(api, threadID, cookieIndex) {
        try {
            if (api && typeof api.getThreadInfo === 'function') {
                api.getThreadInfo(threadID, (err, info) => {
                    if (!err && info) {
                        this.addLog(`Cookie ${cookieIndex + 1}: Target - ${info.name || 'Unknown'} (ID: ${threadID})`, 'info');
                    }
                });
            }
        } catch (e) {}
    }

    startSending() {
        if (!this.config.running) return;
        const activeApis = this.config.apis.filter(api => api !== null);
        if (activeApis.length === 0) {
            this.addLog('No active cookies available', 'error');
            return;
        }

        this.addLog(`Starting message sending with ${activeApis.length} active cookies`, 'info');
        this.sendNextMessage();
    }

    sendNextMessage() {
        if (!this.config.running) return;

        if (this.messageData.currentIndex >= this.messageData.messages.length) {
            this.messageData.loopCount++;
            this.stats.loops = this.messageData.loopCount;
            this.addLog(`Loop #${this.messageData.loopCount} completed. Restarting.`, 'info');
            this.messageData.currentIndex = 0;
        }

        const message = this.messageData.messages[this.messageData.currentIndex];
        const currentIndex = this.messageData.currentIndex;
        const totalMessages = this.messageData.messages.length;

        const api = this.getNextAvailableApi();
        if (!api) {
            this.addLog('No active cookie available, retrying in 10 seconds...', 'warning');
            setTimeout(() => this.sendNextMessage(), 10000);
            return;
        }

        this.sendMessageWithRetry(api, message, currentIndex, totalMessages);
    }

    getNextAvailableApi() {
        const totalCookies = this.config.apis.length;
        for (let attempt = 0; attempt < totalCookies; attempt++) {
            this.currentCookieIndex = (this.currentCookieIndex + 1) % totalCookies;
            const api = this.config.apis[this.currentCookieIndex];
            if (api !== null) {
                this.stats.cookieUsage[this.currentCookieIndex]++;
                return api;
            }
        }
        return null;
    }

    sendMessageWithRetry(api, message, currentIndex, totalMessages, retryAttempt = 0) {
        if (!this.config.running) return;

        const maxSendRetries = 10;
        const cookieNum = this.currentCookieIndex + 1;
        
        try {
            api.sendMessage(message, this.messageData.threadID, (err) => {
                const timestamp = new Date().toLocaleTimeString('en-IN');
                if (err) {
                    this.stats.failed++;
                    if (retryAttempt < maxSendRetries) {
                        this.addLog(`🔄 Cookie ${cookieNum} | RETRY ${retryAttempt + 1}/${maxSendRetries} | Message ${currentIndex + 1}/${totalMessages}`, 'info');
                        setTimeout(() => {
                            this.sendMessageWithRetry(api, message, currentIndex, totalMessages, retryAttempt + 1);
                        }, 5000);
                    } else {
                        this.addLog(`❌ Cookie ${cookieNum} | FAILED after ${maxSendRetries} retries | ${timestamp} | Message ${currentIndex + 1}/${totalMessages}`, 'error');
                        this.config.apis[this.currentCookieIndex] = null;
                        this.stats.activeCookies = this.config.apis.filter(api => api !== null).length;
                        this.messageData.currentIndex++;
                        this.scheduleNextMessage();
                    }
                } else {
                    this.stats.sent++;
                    this.stats.lastSuccess = Date.now();
                    this.retryCount = 0;
                    this.addLog(`✅ Cookie ${cookieNum} | SENT | ${timestamp} | Message ${currentIndex + 1}/${totalMessages} | Loop ${this.messageData.loopCount + 1}`, 'success');
                    this.messageData.currentIndex++;
                    this.scheduleNextMessage();
                }
            });
        } catch (sendError) {
            this.addLog(`🚨 Cookie ${cookieNum} | CRITICAL: Send error - ${sendError.message}`, 'error');
            this.config.apis[this.currentCookieIndex] = null;
            this.stats.activeCookies = this.config.apis.filter(api => api !== null).length;
            this.messageData.currentIndex++;
            this.scheduleNextMessage();
        }
    }

    scheduleNextMessage() {
        if (!this.config.running) return;
        setTimeout(() => {
            try {
                this.sendNextMessage();
            } catch (e) {
                this.addLog(`🚨 Error in message scheduler: ${e.message}`, 'error');
                this.restart();
            }
        }, this.config.delay * 1000);
    }

    restart() {
        this.addLog('🔄 RESTARTING TASK WITH ALL COOKIES...', 'info');
        this.stats.restarts++;
        this.config.restartCount++;
        this.config.apis = [];
        this.stats.activeCookies = 0;
        
        setTimeout(() => {
            if (this.config.running && this.config.restartCount <= this.config.maxRestarts) {
                this.initializeAllBots();
            } else if (this.config.restartCount > this.config.maxRestarts) {
                this.addLog('🚨 MAX RESTARTS REACHED - Task stopped', 'error');
                this.config.running = false;
            }
        }, 10000);
    }

    stop() {
        console.log(`🛑 User ${this.userId} stopping task: ${this.taskId}`);
        this.config.running = false;
        this.stats.activeCookies = 0;
        this.addLog('⏸️ Task stopped by user - IDs remain logged in', 'info');
        this.addLog(`🔢 Total cookies used: ${this.stats.totalCookies}`, 'info');
        this.addLog('🔄 You can use same cookies again without relogin', 'info');
        saveTasksToStorage(); // Auto-save on task stop
        return true;
    }

    getDetails() {
        const activeCookies = this.config.apis.filter(api => api !== null).length;
        const cookieStats = this.cookies.map((cookie, index) => ({
            cookieNumber: index + 1,
            active: this.config.apis[index] !== null,
            messagesSent: this.stats.cookieUsage[index] || 0
        }));
        
        return {
            taskId: this.taskId,
            userId: this.userId,
            sent: this.stats.sent,
            failed: this.stats.failed,
            activeCookies: activeCookies,
            totalCookies: this.stats.totalCookies,
            loops: this.stats.loops,
            restarts: this.stats.restarts,
            cookieStats: cookieStats,
            logs: this.logs.slice(0, 20), // Send only recent logs
            running: this.config.running,
            uptime: this.config.lastActivity ? Date.now() - this.config.lastActivity : 0,
            createdAt: this.stats.createdAt
        };
    }
}

// Helper functions
function broadcastToUser(userId, taskId, message) {
    if (!wss) return;
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && 
            client.userId === userId && 
            client.taskId === taskId) {
            try {
                client.send(JSON.stringify(message));
            } catch (e) {}
        }
    });
}

function broadcastUserTasks(userId) {
    if (!wss) return;
    const user = userTasks.get(userId);
    if (!user) return;
    
    const tasks = user.getAllTasks();
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.userId === userId) {
            try {
                client.send(JSON.stringify({
                    type: 'user_tasks_update',
                    tasks: tasks.map(task => ({
                        taskId: task.taskId,
                        running: task.config.running,
                        stats: task.stats,
                        createdAt: task.stats.createdAt
                    }))
                }));
            } catch (e) {}
        }
    });
}

// Cleanup inactive users
function cleanupInactiveUsers() {
    const now = Date.now();
    for (let [userId, user] of userTasks.entries()) {
        if (!user.healthCheck()) {
            console.log(`🧹 Cleaning up inactive user: ${userId}`);
            user.stopAllTasks();
            userTasks.delete(userId);
            userSessions.delete(userId);
        }
    }
    saveTasksToStorage();
}

// Load tasks from storage on startup
loadTasksFromStorage();

// Complete HTML with improved file upload handling
const htmlLoginPanel = `
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>FAIZU MULTI-COOKIE MESSENGER</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');
  
  * {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
    font-family: 'Inter', sans-serif;
  }
  
  :root {
    --primary-green: #14532d;
    --secondary-green: #166534;
    --light-green: #bbf7d0;
    --dark-green: #052e16;
    --accent-teal: #0d9488;
    --accent-lime: #65a30d;
    --accent-emerald: #10b981;
    --accent-amber: #d97706;
    --accent-rose: #dc2626;
    --bg-dark: #0a0a0a;
    --bg-light: #1a1a1a;
    --text-light: #ffffff;
  }
  
  body {
    background: var(--bg-dark);
    color: var(--text-light);
    min-height: 100vh;
    overflow-x: hidden;
    -webkit-tap-highlight-color: transparent;
  }
  
  /* Login Container */
  .login-container {
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 100vh;
    padding: 15px;
    background: linear-gradient(rgba(10, 10, 10, 0.9), rgba(10, 10, 10, 0.9)), 
                url('https://i.pinimg.com/originals/c5/a6/17/c5a617ac4baf3f1fb844d5487486fe58.gif');
    background-size: cover;
    background-position: center;
  }
  
  .login-box {
    background: rgba(20, 83, 45, 0.9);
    border: 1px solid var(--accent-emerald);
    border-radius: 12px;
    padding: 25px;
    width: 100%;
    max-width: 350px;
    backdrop-filter: blur(5px);
  }
  
  .login-header {
    text-align: center;
    margin-bottom: 20px;
  }
  
  .login-header h1 {
    font-size: 20px;
    color: var(--light-green);
    margin-bottom: 5px;
  }
  
  .login-header p {
    color: var(--light-green);
    opacity: 0.8;
    font-size: 11px;
  }
  
  .login-form {
    display: flex;
    flex-direction: column;
    gap: 15px;
  }
  
  .form-group label {
    display: block;
    color: var(--light-green);
    margin-bottom: 5px;
    font-size: 12px;
  }
  
  .form-input {
    width: 100%;
    padding: 10px 12px;
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid rgba(16, 185, 129, 0.3);
    border-radius: 6px;
    color: var(--text-light);
    font-size: 13px;
  }
  
  .form-input:focus {
    outline: none;
    border-color: var(--accent-emerald);
  }
  
  .login-btn {
    background: var(--accent-emerald);
    color: white;
    border: none;
    padding: 11px;
    border-radius: 6px;
    font-size: 13px;
    cursor: pointer;
    margin-top: 5px;
  }
  
  .login-footer {
    text-align: center;
    margin-top: 15px;
    color: var(--light-green);
    font-size: 10px;
    opacity: 0.7;
  }
  
  /* Dashboard */
  .dashboard {
    display: none;
    padding: 15px;
    min-height: 100vh;
    background: linear-gradient(rgba(10, 10, 10, 0.9), rgba(10, 10, 10, 0.9)), 
                url('https://i.pinimg.com/originals/92/38/06/92380655e6a2ef7f948311fa74686cdc.gif');
    background-size: cover;
    background-position: center;
  }
  
  .header {
    background: rgba(20, 83, 45, 0.8);
    border: 1px solid var(--accent-emerald);
    border-radius: 10px;
    padding: 15px;
    margin-bottom: 20px;
  }
  
  .header-content {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  
  .header-title h1 {
    font-size: 16px;
    color: var(--light-green);
  }
  
  .header-title p {
    color: var(--light-green);
    opacity: 0.8;
    font-size: 11px;
  }
  
  .user-info {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  
  .logout-btn {
    background: var(--accent-rose);
    color: white;
    border: none;
    padding: 6px 12px;
    border-radius: 4px;
    font-size: 11px;
    cursor: pointer;
  }
  
  /* Control Panels */
  .control-panels {
    display: grid;
    grid-template-columns: 1fr;
    gap: 15px;
    margin-bottom: 20px;
  }
  
  .panel {
    background: rgba(20, 83, 45, 0.8);
    border: 1px solid var(--accent-emerald);
    border-radius: 10px;
    padding: 15px;
  }
  
  .panel-title {
    color: var(--light-green);
    font-size: 14px;
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid rgba(16, 185, 129, 0.3);
  }
  
  .form-row {
    margin-bottom: 12px;
  }
  
  .form-row label {
    display: block;
    color: var(--light-green);
    margin-bottom: 5px;
    font-size: 12px;
  }
  
  .form-textarea {
    width: 100%;
    padding: 10px;
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid rgba(16, 185, 129, 0.3);
    border-radius: 6px;
    color: var(--text-light);
    font-size: 12px;
    min-height: 60px;
    resize: vertical;
  }
  
  .radio-group {
    display: flex;
    gap: 15px;
    margin-bottom: 10px;
  }
  
  .radio-group label {
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 12px;
    color: var(--light-green);
  }
  
  /* IMPROVED FILE UPLOAD STYLING */
  .file-upload-wrapper {
    position: relative;
    width: 100%;
    margin-bottom: 10px;
  }
  
  .file-upload-btn {
    display: block;
    width: 100%;
    padding: 12px;
    background: rgba(16, 185, 129, 0.2);
    border: 2px dashed rgba(16, 185, 129, 0.4);
    border-radius: 8px;
    color: var(--light-green);
    text-align: center;
    cursor: pointer;
    transition: all 0.3s ease;
    font-size: 13px;
  }
  
  .file-upload-btn:hover {
    background: rgba(16, 185, 129, 0.3);
    border-color: var(--accent-emerald);
  }
  
  .file-upload-btn i {
    margin-right: 8px;
  }
  
  .file-input-hidden {
    position: absolute;
    width: 100%;
    height: 100%;
    top: 0;
    left: 0;
    opacity: 0;
    cursor: pointer;
    z-index: 10;
  }
  
  .file-info {
    margin-top: 8px;
    font-size: 11px;
    color: var(--light-green);
    opacity: 0.8;
  }
  
  .file-selected {
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid var(--accent-emerald);
    border-radius: 6px;
    padding: 8px 12px;
    margin-top: 8px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 12px;
  }
  
  .file-name {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  
  .file-remove {
    background: var(--accent-rose);
    color: white;
    border: none;
    border-radius: 4px;
    padding: 2px 6px;
    font-size: 10px;
    cursor: pointer;
    margin-left: 8px;
  }
  
  .form-buttons {
    display: flex;
    gap: 10px;
    margin-top: 15px;
  }
  
  .btn {
    padding: 8px 15px;
    border-radius: 6px;
    border: none;
    cursor: pointer;
    font-size: 12px;
    flex: 1;
    transition: all 0.3s ease;
  }
  
  .btn-primary {
    background: var(--accent-emerald);
    color: white;
  }
  
  .btn-primary:hover {
    background: #0fa674;
    transform: translateY(-1px);
  }
  
  .btn-secondary {
    background: rgba(16, 185, 129, 0.2);
    color: var(--light-green);
    border: 1px solid rgba(16, 185, 129, 0.3);
  }
  
  .btn-secondary:hover {
    background: rgba(16, 185, 129, 0.3);
  }
  
  /* Stats Panel */
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 10px;
    margin-top: 10px;
  }
  
  .stat-card {
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid rgba(16, 185, 129, 0.2);
    border-radius: 6px;
    padding: 12px;
    text-align: center;
    transition: all 0.3s ease;
  }
  
  .stat-card:hover {
    transform: translateY(-2px);
    border-color: var(--accent-emerald);
  }
  
  .stat-value {
    font-size: 18px;
    font-weight: 600;
    color: var(--accent-emerald);
    margin-bottom: 3px;
  }
  
  .stat-label {
    font-size: 10px;
    color: var(--light-green);
    opacity: 0.8;
  }
  
  /* My Tasks Panel */
  .tasks-panel {
    background: rgba(20, 83, 45, 0.8);
    border: 1px solid var(--accent-emerald);
    border-radius: 10px;
    padding: 15px;
    margin-bottom: 15px;
  }
  
  .tasks-list {
    max-height: 200px;
    overflow-y: auto;
    margin-top: 10px;
  }
  
  .task-item {
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid rgba(16, 185, 129, 0.2);
    border-radius: 6px;
    padding: 10px;
    margin-bottom: 8px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: all 0.3s ease;
  }
  
  .task-item:hover {
    border-color: var(--accent-emerald);
    transform: translateX(2px);
  }
  
  .task-info {
    flex: 1;
  }
  
  .task-id {
    font-size: 12px;
    color: var(--accent-emerald);
    font-weight: 500;
  }
  
  .task-stats {
    font-size: 10px;
    color: var(--light-green);
    opacity: 0.8;
    margin-top: 3px;
  }
  
  .task-actions {
    display: flex;
    gap: 5px;
  }
  
  .task-btn {
    padding: 4px 8px;
    border-radius: 4px;
    border: none;
    font-size: 10px;
    cursor: pointer;
    transition: all 0.3s ease;
  }
  
  .task-btn-view {
    background: var(--accent-emerald);
    color: white;
  }
  
  .task-btn-view:hover {
    background: #0fa674;
  }
  
  .task-btn-stop {
    background: var(--accent-rose);
    color: white;
  }
  
  .task-btn-stop:hover {
    background: #c53030;
  }
  
  /* Console Panel */
  .console-tabs {
    display: flex;
    gap: 8px;
    margin-bottom: 12px;
    border-bottom: 1px solid rgba(16, 185, 129, 0.3);
    padding-bottom: 8px;
  }
  
  .console-tab {
    padding: 6px 12px;
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid rgba(16, 185, 129, 0.3);
    border-radius: 4px;
    color: var(--light-green);
    cursor: pointer;
    font-size: 11px;
    transition: all 0.3s ease;
  }
  
  .console-tab.active {
    background: var(--accent-emerald);
    color: white;
    border-color: var(--accent-emerald);
  }
  
  .console-tab:hover:not(.active) {
    background: rgba(16, 185, 129, 0.2);
  }
  
  .console-content {
    display: none;
  }
  
  .console-content.active {
    display: block;
  }
  
  .console-log {
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid rgba(16, 185, 129, 0.2);
    border-radius: 6px;
    padding: 12px;
    height: 150px;
    overflow-y: auto;
    font-size: 11px;
  }
  
  .log-entry {
    margin-bottom: 6px;
    padding: 5px 8px;
    border-left: 2px solid var(--accent-emerald);
    background: rgba(16, 185, 129, 0.05);
    font-size: 10px;
    animation: fadeIn 0.3s ease;
  }
  
  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(-5px); }
    to { opacity: 1; transform: translateY(0); }
  }
  
  .log-time {
    color: var(--accent-emerald);
    margin-right: 8px;
    font-size: 9px;
  }
  
  .task-id-display {
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid var(--accent-emerald);
    border-radius: 6px;
    padding: 12px;
    margin-top: 15px;
    text-align: center;
    animation: pulse 2s infinite;
  }
  
  @keyframes pulse {
    0% { box-shadow: 0 0 5px rgba(16, 185, 129, 0.3); }
    50% { box-shadow: 0 0 10px rgba(16, 185, 129, 0.5); }
    100% { box-shadow: 0 0 5px rgba(16, 185, 129, 0.3); }
  }
  
  .task-id-value {
    font-size: 13px;
    color: var(--accent-emerald);
    word-break: break-all;
    margin: 5px 0;
    font-weight: 500;
  }
  
  /* Auto-save notification */
  .save-notification {
    position: fixed;
    bottom: 20px;
    right: 20px;
    background: var(--accent-emerald);
    color: white;
    padding: 8px 15px;
    border-radius: 6px;
    font-size: 11px;
    opacity: 0;
    transform: translateY(20px);
    transition: all 0.3s ease;
    z-index: 1000;
  }
  
  .save-notification.show {
    opacity: 1;
    transform: translateY(0);
  }
  
  /* Responsive */
  @media (min-width: 768px) {
    .control-panels {
      grid-template-columns: repeat(2, 1fr);
    }
    
    .tasks-panel {
      grid-column: 1 / -1;
    }
    
    .console-panel {
      grid-column: 1 / -1;
    }
    
    .login-box {
      max-width: 400px;
      padding: 30px;
    }
    
    .dashboard {
      padding: 20px;
    }
  }
  
  /* Scrollbar */
  ::-webkit-scrollbar {
    width: 6px;
  }
  
  ::-webkit-scrollbar-track {
    background: rgba(16, 185, 129, 0.1);
    border-radius: 3px;
  }
  
  ::-webkit-scrollbar-thumb {
    background: var(--accent-emerald);
    border-radius: 3px;
  }
  
  ::-webkit-scrollbar-thumb:hover {
    background: #0fa674;
  }
  
  /* No text selection */
  .no-select {
    -webkit-user-select: none;
    -moz-user-select: none;
    -ms-user-select: none;
    user-select: none;
  }
</style>
</head>
<body>
  <!-- Login Screen -->
  <div class="login-container" id="loginContainer">
    <div class="login-box">
      <div class="login-header">
        <h1>FAIZU COOKIE SERVER</h1>
        <p>Multi-cookie messenger system</p>
        <p style="font-size: 10px; margin-top: 3px;">By Faizu • v2.0</p>
      </div>
      
      <form class="login-form" id="loginForm">
        <div class="form-group">
          <label for="username">Username</label>
          <input type="text" id="username" class="form-input" placeholder="Enter username" required>
        </div>
        
        <div class="form-group">
          <label for="password">Password</label>
          <input type="password" id="password" class="form-input" placeholder="Enter password" required>
        </div>
        
        <button type="submit" class="login-btn">Login</button>
      </form>
      
      <div class="login-footer">
        <p>Secure • Multi-cookie • Auto-recovery</p>
      </div>
    </div>
  </div>
  
  <!-- Main Dashboard -->
  <div class="dashboard" id="dashboard">
    <!-- Header -->
    <div class="header">
      <div class="header-content">
        <div class="header-title">
          <h1>FAIZU COOKIE SERVER</h1>
          <p>Multi-cookie messenger control panel</p>
        </div>
        
        <div class="user-info">
          <span id="welcomeUser" style="font-size: 12px;">User</span>
          <button class="logout-btn" id="logoutBtn">Logout</button>
        </div>
      </div>
    </div>
    
    <!-- My Tasks Panel -->
    <div class="tasks-panel" id="tasksPanel" style="display: none;">
      <div class="panel-title">📋 My Tasks (Auto-saved)</div>
      <div class="tasks-list" id="myTasksList">
        <!-- Tasks will be loaded here -->
      </div>
    </div>
    
    <!-- Control Panels -->
    <div class="control-panels">
      <!-- Configuration Panel -->
      <div class="panel">
        <div class="panel-title">Settings</div>
        
        <div class="form-row">
          <label>Cookie Mode:</label>
          <div class="radio-group">
            <label class="no-select">
              <input type="radio" name="cookieMode" value="file" checked> File
            </label>
            <label class="no-select">
              <input type="radio" name="cookieMode" value="paste"> Paste
            </label>
          </div>
        </div>
        
        <div id="cookieFileSection">
          <div class="form-row">
            <label>Cookie File (.txt/.json):</label>
            <div class="file-upload-wrapper">
              <div class="file-upload-btn no-select">
                <i class="fas fa-file-upload"></i> Click to upload cookie file
              </div>
              <input type="file" id="cookieFile" accept=".txt,.json" class="file-input-hidden">
              <div class="file-info">
                One cookie per line • Multiple cookies supported
              </div>
              <div id="cookieFileInfo" class="file-selected" style="display: none;">
                <span class="file-name"></span>
                <button type="button" class="file-remove no-select" onclick="removeCookieFile()">×</button>
              </div>
            </div>
          </div>
        </div>
        
        <div id="cookiePasteSection" style="display: none;">
          <div class="form-row">
            <label>Paste Cookies:</label>
            <textarea id="cookiePaste" class="form-textarea" placeholder="Paste cookies here - one per line"></textarea>
          </div>
        </div>
        
        <div class="form-row">
          <label>Prefix Name:</label>
          <input type="text" id="hatersName" class="form-input" placeholder="Hater's name">
        </div>
        
        <div class="form-row">
          <label>Suffix Name:</label>
          <input type="text" id="lastHereName" class="form-input" placeholder="Last here name">
        </div>
      </div>
      
      <!-- Message Settings -->
      <div class="panel">
        <div class="panel-title">Message Settings</div>
        
        <div class="form-row">
          <label>Thread/Group ID:</label>
          <input type="text" id="threadID" class="form-input" placeholder="Target ID">
        </div>
        
        <div class="form-row">
          <label>Message File (.txt):</label>
          <div class="file-upload-wrapper">
            <div class="file-upload-btn no-select">
              <i class="fas fa-file-upload"></i> Click to upload message file
            </div>
            <input type="file" id="messageFile" accept=".txt" class="file-input-hidden">
            <div class="file-info">
              One message per line • Auto-loop enabled
            </div>
            <div id="messageFileInfo" class="file-selected" style="display: none;">
              <span class="file-name"></span>
              <button type="button" class="file-remove no-select" onclick="removeMessageFile()">×</button>
            </div>
          </div>
        </div>
        
        <div class="form-row">
          <label>Delay (seconds):</label>
          <input type="number" id="delay" class="form-input" value="5" min="1" max="60">
        </div>
        
        <div class="form-buttons">
          <button type="button" id="startBtn" class="btn btn-primary">Start New Task</button>
          <button type="button" id="stopAllBtn" class="btn btn-secondary">Stop All My Tasks</button>
        </div>
      </div>
      
      <!-- Stats Panel -->
      <div class="panel">
        <div class="panel-title">📊 Current Task Stats</div>
        
        <div class="stats-grid">
          <div class="stat-card">
            <div class="stat-value" id="statSent">0</div>
            <div class="stat-label">Sent</div>
          </div>
          <div class="stat-card">
            <div class="stat-value" id="statFailed">0</div>
            <div class="stat-label">Failed</div>
          </div>
          <div class="stat-card">
            <div class="stat-value" id="statActive">0</div>
            <div class="stat-label">Active</div>
          </div>
          <div class="stat-card">
            <div class="stat-value" id="statLoops">0</div>
            <div class="stat-label">Loops</div>
          </div>
        </div>
        
        <div id="taskIdDisplay" class="task-id-display" style="display: none;">
          <div style="font-size: 12px; color: var(--light-green);">Current Task ID:</div>
          <div class="task-id-value" id="currentTaskId"></div>
          <div style="font-size: 10px; color: var(--light-green); opacity: 0.7; margin-top: 5px;">
            Task auto-saved to storage
          </div>
        </div>
      </div>
      
      <!-- Console Panel -->
      <div class="panel console-panel">
        <div class="panel-title">📝 Console Logs</div>
        
        <div class="console-tabs">
          <div class="console-tab active" onclick="switchTab('liveLogs')">Live Logs</div>
          <div class="console-tab" onclick="switchTab('viewTask')">View Task</div>
          <div class="console-tab" onclick="switchTab('taskHistory')">Task History</div>
        </div>
        
        <!-- Live Logs -->
        <div id="liveLogs" class="console-content active">
          <div class="console-log" id="liveConsole"></div>
        </div>
        
        <!-- View Task -->
        <div id="viewTask" class="console-content">
          <div class="form-row">
            <label>Enter Task ID (from My Tasks):</label>
            <input type="text" id="viewTaskId" class="form-input" placeholder="Enter task ID">
          </div>
          <button type="button" id="viewTaskBtn" class="btn btn-primary" style="margin-top: 10px;">View Task Details</button>
          
          <div id="taskDetails" style="margin-top: 15px; display: none;">
            <div style="border: 1px solid var(--accent-emerald); border-radius: 6px; padding: 10px;">
              <div id="detailStats" class="stats-grid"></div>
            </div>
          </div>
        </div>
        
        <!-- Task History -->
        <div id="taskHistory" class="console-content">
          <div style="font-size: 11px; color: var(--light-green); opacity: 0.8; margin-bottom: 10px;">
            Your task history will be shown here
          </div>
          <div id="historyList" class="console-log" style="height: 120px;"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- Auto-save Notification -->
  <div class="save-notification" id="saveNotification">
    <i class="fas fa-save"></i> Tasks auto-saved
  </div>

  <!-- Font Awesome for icons -->
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">

<script>
  let currentUserId = null;
  let currentTaskId = null;
  let ws = null;
  let userSessionId = null;
  let reconnectAttempts = 0;
  const MAX_RECONNECT_ATTEMPTS = 5;
  
  // Store user data in localStorage for persistence
  function saveUserData() {
    if (currentUserId) {
      localStorage.setItem('faizu_userId', currentUserId);
      localStorage.setItem('faizu_lastLogin', Date.now());
    }
  }
  
  function loadUserData() {
    const savedUserId = localStorage.getItem('faizu_userId');
    const lastLogin = localStorage.getItem('faizu_lastLogin');
    
    // Check if last login was within 24 hours
    if (savedUserId && lastLogin && (Date.now() - parseInt(lastLogin)) < 24 * 60 * 60 * 1000) {
      return savedUserId;
    }
    return null;
  }
  
  function clearUserData() {
    localStorage.removeItem('faizu_userId');
    localStorage.removeItem('faizu_lastLogin');
  }
  
  // Show save notification
  function showSaveNotification() {
    const notification = document.getElementById('saveNotification');
    notification.classList.add('show');
    setTimeout(() => {
      notification.classList.remove('show');
    }, 2000);
  }
  
  // Tab switching
  function switchTab(tabName) {
    document.querySelectorAll('.console-content').forEach(tab => {
      tab.classList.remove('active');
    });
    document.querySelectorAll('.console-tab').forEach(tab => {
      tab.classList.remove('active');
    });
    
    document.getElementById(tabName).classList.add('active');
    event.target.classList.add('active');
  }
  
  // Cookie mode toggle
  document.querySelectorAll('input[name="cookieMode"]').forEach(radio => {
    radio.addEventListener('change', function() {
      if (this.value === 'file') {
        document.getElementById('cookieFileSection').style.display = 'block';
        document.getElementById('cookiePasteSection').style.display = 'none';
      } else {
        document.getElementById('cookieFileSection').style.display = 'none';
        document.getElementById('cookiePasteSection').style.display = 'block';
      }
    });
  });
  
  // Improved file upload handling
  document.getElementById('cookieFile').addEventListener('change', function(e) {
    if (this.files.length > 0) {
      const file = this.files[0];
      const fileInfo = document.getElementById('cookieFileInfo');
      const fileName = fileInfo.querySelector('.file-name');
      fileName.textContent = file.name;
      fileInfo.style.display = 'flex';
    }
  });
  
  document.getElementById('messageFile').addEventListener('change', function(e) {
    if (this.files.length > 0) {
      const file = this.files[0];
      const fileInfo = document.getElementById('messageFileInfo');
      const fileName = fileInfo.querySelector('.file-name');
      fileName.textContent = file.name;
      fileInfo.style.display = 'flex';
    }
  });
  
  function removeCookieFile() {
    document.getElementById('cookieFile').value = '';
    document.getElementById('cookieFileInfo').style.display = 'none';
  }
  
  function removeMessageFile() {
    document.getElementById('messageFile').value = '';
    document.getElementById('messageFileInfo').style.display = 'none';
  }
  
  // Login
  document.getElementById('loginForm').addEventListener('submit', function(e) {
    e.preventDefault();
    
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    
    // Generate unique user ID based on username and timestamp
    const savedUserId = loadUserData();
    if (savedUserId) {
      currentUserId = savedUserId;
    } else {
      currentUserId = 'USER-' + Date.now() + '-' + Math.random().toString(36).substr(2, 6);
      saveUserData();
    }
    
    if (username === 'Faizu Xd' && password === 'Justfuckaway3') {
      document.getElementById('loginContainer').style.display = 'none';
      document.getElementById('dashboard').style.display = 'block';
      document.getElementById('welcomeUser').textContent = username + ' (' + currentUserId.substr(0, 10) + '...)';
      document.getElementById('tasksPanel').style.display = 'block';
      
      // Connect WebSocket
      connectWebSocket();
      
      addLog('✅ Login successful - User ID: ' + currentUserId);
      addLog('🔗 Connecting to server...');
    } else {
      alert('❌ Invalid! Use: Faizu Xd / Justfuckaway3');
    }
  });
  
  // WebSocket connection with reconnection
  function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = protocol + '//' + window.location.host;
    
    ws = new WebSocket(wsUrl);
    
    ws.onopen = function() {
      reconnectAttempts = 0;
      addLog('🔗 Connected to server');
      // Send user identification
      ws.send(JSON.stringify({
        type: 'user_identify',
        userId: currentUserId
      }));
    };
    
    ws.onmessage = function(event) {
      try {
        const data = JSON.parse(event.data);
        
        if (data.type === 'user_identified') {
          addLog('✅ User session created');
          loadUserTasks();
        }
        else if (data.type === 'user_tasks_update') {
          updateMyTasksList(data.tasks);
        }
        else if (data.type === 'task_started') {
          currentTaskId = data.taskId;
          document.getElementById('currentTaskId').textContent = currentTaskId;
          document.getElementById('taskIdDisplay').style.display = 'block';
          addLog('🚀 Task started: ' + currentTaskId);
          loadUserTasks();
          showSaveNotification();
        }
        else if (data.type === 'task_stopped') {
          if (data.taskId === currentTaskId) {
            document.getElementById('taskIdDisplay').style.display = 'none';
          }
          addLog('⏹️ Task stopped: ' + data.taskId);
          loadUserTasks();
          showSaveNotification();
        }
        else if (data.type === 'log') {
          addLog(data.message);
        }
        else if (data.type === 'stats_update') {
          updateStats(data);
        }
        else if (data.type === 'task_details') {
          showTaskDetails(data);
        }
        else if (data.type === 'error') {
          addLog('❌ Error: ' + data.message);
        }
        else if (data.type === 'save_notification') {
          showSaveNotification();
        }
      } catch (e) {
        console.error('Error parsing WebSocket message:', e);
      }
    };
    
    ws.onclose = function(event) {
      addLog('🔌 Disconnected from server');
      if (event.code !== 1000 && reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttempts++;
        addLog(
  "Reconnecting... (" +
  reconnectAttempts +
  "/" +
  MAX_RECONNECT_ATTEMPTS +
  ")"
);
        setTimeout(connectWebSocket, 3000);
      }
    };
    
    ws.onerror = function() {
      addLog('⚠️ WebSocket error');
    };
  }
  
  // Load user tasks
  function loadUserTasks() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'get_user_tasks',
        userId: currentUserId
      }));
    }
  }
  
  // Update my tasks list
  function updateMyTasksList(tasks) {
    const tasksList = document.getElementById('myTasksList');
    if (tasks.length === 0) {
      tasksList.innerHTML = '<div style="text-align: center; color: var(--light-green); opacity: 0.7; font-size: 11px; padding: 20px;">No tasks yet</div>';
      return;
    }
    
    let html = '';
    tasks.forEach(task => {
      html += \`
        <div class="task-item">
          <div class="task-info">
            <div class="task-id">\${task.taskId}</div>
            <div class="task-stats">
              Sent: \${task.stats?.sent || 0} | 
              Active: \${task.stats?.activeCookies || 0}/\${task.stats?.totalCookies || 0} | 
              Status: \${task.running ? '🟢 Running' : '⏸️ Paused'}
            </div>
          </div>
          <div class="task-actions">
            <button class="task-btn task-btn-view" onclick="viewMyTask('\${task.taskId}')">View</button>
            <button class="task-btn task-btn-stop" onclick="stopMyTask('\${task.taskId}')">Stop</button>
          </div>
        </div>
      \`;
    });
    
    tasksList.innerHTML = html;
  }
  
  // View my task
  function viewMyTask(taskId) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'view_details',
        taskId: taskId,
        userId: currentUserId
      }));
      switchTab('viewTask');
      document.getElementById('viewTaskId').value = taskId;
    }
  }
  
  // Stop my task
  function stopMyTask(taskId) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'stop',
        taskId: taskId,
        userId: currentUserId
      }));
    }
  }
  
  // Logout
  document.getElementById('logoutBtn').addEventListener('click', function() {
    // Stop all tasks before logout
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'logout',
        userId: currentUserId
      }));
      ws.close();
    }
    
    document.getElementById('dashboard').style.display = 'none';
    document.getElementById('loginContainer').style.display = 'flex';
    clearUserData();
    currentUserId = null;
    currentTaskId = null;
    document.getElementById('username').value = '';
    document.getElementById('password').value = '';
    document.getElementById('tasksPanel').style.display = 'none';
    addLog('🔒 Logged out');
  });
  
  // Add log
  function addLog(message) {
    const console = document.getElementById('liveConsole');
    const logEntry = document.createElement('div');
    logEntry.className = 'log-entry';
    
    const time = new Date().toLocaleTimeString('en-IN', {hour12: false});
    logEntry.innerHTML = '<span class="log-time">[' + time + ']</span>' + message;
    
    console.appendChild(logEntry);
    console.scrollTop = console.scrollHeight;
  }
  
  // Update stats
  function updateStats(data) {
    document.getElementById('statSent').textContent = data.sent || 0;
    document.getElementById('statFailed').textContent = data.failed || 0;
    document.getElementById('statActive').textContent = data.activeCookies || 0;
    document.getElementById('statLoops').textContent = data.loops || 0;
  }
  
  // Show task details
  function showTaskDetails(data) {
    const detailsDiv = document.getElementById('taskDetails');
    const statsDiv = document.getElementById('detailStats');
    
    statsDiv.innerHTML = \`
      <div class="stat-card">
        <div class="stat-value">\${data.sent || 0}</div>
        <div class="stat-label">Sent</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">\${data.failed || 0}</div>
        <div class="stat-label">Failed</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">\${data.activeCookies || 0}</div>
        <div class="stat-label">Active</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">\${data.loops || 0}</div>
        <div class="stat-label">Loops</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">\${data.totalCookies || 0}</div>
        <div class="stat-label">Total</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">\${data.restarts || 0}</div>
        <div class="stat-label">Restarts</div>
      </div>
    \`;
    
    detailsDiv.style.display = 'block';
  }
  
  // Start new task
  document.getElementById('startBtn').addEventListener('click', function() {
    const cookieMode = document.querySelector('input[name="cookieMode"]:checked').value;
    const hatersName = document.getElementById('hatersName').value;
    const lastHereName = document.getElementById('lastHereName').value;
    const threadID = document.getElementById('threadID').value;
    const delay = document.getElementById('delay').value;
    
    if (!threadID) {
      addLog('❌ Enter Thread ID');
      return;
    }
    
    if (!hatersName || !lastHereName) {
      addLog('❌ Enter both prefix and suffix names');
      return;
    }
    
    // Get cookie content
    let cookieContent = '';
    if (cookieMode === 'file') {
      const fileInput = document.getElementById('cookieFile');
      if (fileInput.files.length === 0) {
        addLog('❌ Select cookie file');
        return;
      }
      const reader = new FileReader();
      reader.onload = function(e) {
        cookieContent = e.target.result;
        sendStartTask(cookieContent, hatersName, lastHereName, threadID, delay);
      };
      reader.onerror = function() {
        addLog('❌ Error reading cookie file');
      };
      reader.readAsText(fileInput.files[0]);
    } else {
      cookieContent = document.getElementById('cookiePaste').value;
      if (!cookieContent.trim()) {
        addLog('❌ Paste cookies');
        return;
      }
      sendStartTask(cookieContent, hatersName, lastHereName, threadID, delay);
    }
  });
  
  // Send start task request
  function sendStartTask(cookieContent, hatersName, lastHereName, threadID, delay) {
    // Get message content
    const messageFile = document.getElementById('messageFile');
    if (messageFile.files.length === 0) {
      addLog('❌ Select message file');
      return;
    }
    
    const reader = new FileReader();
    reader.onload = function(e) {
      const messageContent = e.target.result;
      
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'start',
          userId: currentUserId,
          cookieContent: cookieContent,
          messageContent: messageContent,
          hatersName: hatersName,
          threadID: threadID,
          lastHereName: lastHereName,
          delay: parseInt(delay) || 5
        }));
        
        addLog('⏳ Starting new task...');
        
        // Clear file inputs after sending
        removeCookieFile();
        removeMessageFile();
        document.getElementById('cookiePaste').value = '';
        document.getElementById('hatersName').value = '';
        document.getElementById('lastHereName').value = '';
        document.getElementById('threadID').value = '';
      } else {
        addLog('❌ Not connected to server');
      }
    };
    reader.onerror = function() {
      addLog('❌ Error reading message file');
    };
    reader.readAsText(messageFile.files[0]);
  }
  
  // Stop all my tasks
  document.getElementById('stopAllBtn').addEventListener('click', function() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'stop_all',
        userId: currentUserId
      }));
      addLog('🛑 Stopping all my tasks...');
    }
  });
  
  // View task button
  document.getElementById('viewTaskBtn').addEventListener('click', function() {
    const taskId = document.getElementById('viewTaskId').value.trim();
    if (taskId && ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'view_details',
        taskId: taskId,
        userId: currentUserId
      }));
    } else {
      addLog('❌ Enter task ID');
    }
  });
  
  // Auto-reconnect on page visibility change
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'visible' && currentUserId && (!ws || ws.readyState !== WebSocket.OPEN)) {
      addLog('🔄 Page visible, reconnecting...');
      connectWebSocket();
    }
  });
  
  // Prevent accidental page refresh
  window.addEventListener('beforeunload', function(e) {
    if (currentUserId) {
      // Don't show dialog, just save data
      saveUserData();
    }
  });
  
  // Auto-login if previously logged in
  window.addEventListener('load', function() {
    const savedUserId = loadUserData();
    if (savedUserId) {
      document.getElementById('username').value = 'Faizu Xd';
      document.getElementById('password').value = 'Justfuckaway3';
      setTimeout(() => {
        document.getElementById('loginForm').dispatchEvent(new Event('submit'));
      }, 100);
    } else {
      setTimeout(() => {
        addLog('✅ System ready - Login to start');
      }, 500);
    }
  });
</script>
</body>
</html>
`;

// Set up Express server
app.use(express.static('public'));
app.get('/', (req, res) => {
  res.send(htmlLoginPanel);
});

// File upload endpoints
app.post('/api/upload/cookie', upload.single('cookieFile'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }
  
  try {
    const content = fs.readFileSync(req.file.path, 'utf8');
    res.json({
      success: true,
      filename: req.file.filename,
      content: content
    });
    
    // Clean up file
    fs.unlinkSync(req.file.path);
  } catch (error) {
    res.status(500).json({ error: 'Error reading file' });
  }
});

app.post('/api/upload/message', upload.single('messageFile'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }
  
  try {
    const content = fs.readFileSync(req.file.path, 'utf8');
    res.json({
      success: true,
      filename: req.file.filename,
      content: content
    });
    
    // Clean up file
    fs.unlinkSync(req.file.path);
  } catch (error) {
    res.status(500).json({ error: 'Error reading file' });
  }
});

// API endpoint to get user tasks
app.get('/api/user/:userId/tasks', (req, res) => {
  const userId = req.params.userId;
  const user = userTasks.get(userId);
  
  if (!user) {
    return res.json({ tasks: [] });
  }
  
  const tasks = user.getAllTasks().map(task => ({
    taskId: task.taskId,
    running: task.config.running,
    stats: task.stats,
    createdAt: task.stats.createdAt
  }));
  
  res.json({ tasks: tasks });
});

// Start server
const server = app.listen(PORT, () => {
  console.log(`🚀 Faizu Multi-User System running at http://localhost:${PORT}`);
  console.log(`🔐 Login Credentials: Faizu Xd / Justfuckaway3`);
  console.log(`💾 Persistent storage: ENABLED (saved to ${STORAGE_FILE})`);
  console.log(`📁 File uploads: ENABLED`);
  console.log(`🔄 Auto-reconnect: ENABLED`);
  console.log(`💪 Tasks preserved on refresh`);
  
  setupConsoleClear();
  setInterval(cleanupInactiveUsers, 5 * 60 * 1000); // Cleanup every 5 minutes
  setInterval(saveTasksToStorage, 60 * 1000); // Auto-save every minute
});

// WebSocket server
let wss = new WebSocket.Server({ server, 
  clientTracking: true,
  perMessageDeflate: {
    zlibDeflateOptions: {
      chunkSize: 1024,
      memLevel: 7,
      level: 3
    },
    zlibInflateOptions: {
      chunkSize: 10 * 1024
    },
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    serverMaxWindowBits: 10,
    concurrencyLimit: 10,
    threshold: 1024
  }
});

wss.on('connection', (ws, req) => {
  ws.userId = null;
  ws.taskId = null;
  ws.isAlive = true;
  
  // Handle pong
  ws.on('pong', () => {
    ws.isAlive = true;
  });
  
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      // User identification
      if (data.type === 'user_identify') {
        ws.userId = data.userId;
        if (!userTasks.has(data.userId)) {
          userTasks.set(data.userId, new User(data.userId));
        }
        userSessions.set(data.userId, Date.now());
        ws.send(JSON.stringify({ 
          type: 'user_identified',
          userId: data.userId
        }));
        broadcastUserTasks(data.userId);
        console.log(`👤 User connected: ${data.userId}`);
      }
      
      // Get user tasks
      else if (data.type === 'get_user_tasks' && ws.userId) {
        const user = userTasks.get(ws.userId);
        if (user) {
          const tasks = user.getAllTasks();
          ws.send(JSON.stringify({
            type: 'user_tasks_update',
            tasks: tasks.map(task => ({
              taskId: task.taskId,
              running: task.config.running,
              stats: task.stats,
              createdAt: task.stats.createdAt
            }))
          }));
        }
      }
      
      // Start task
      else if (data.type === 'start' && ws.userId) {
        const taskId = uuidv4();
        ws.taskId = taskId;
        
        let user = userTasks.get(data.userId);
        if (!user) {
          user = new User(data.userId);
          userTasks.set(data.userId, user);
        }
        
        const task = new Task(taskId, {
          cookieContent: data.cookieContent,
          messageContent: data.messageContent,
          hatersName: data.hatersName,
          threadID: data.threadID,
          lastHereName: data.lastHereName,
          delay: data.delay
        }, data.userId);
        
        if (task.start()) {
          user.addTask(taskId, task);
          ws.send(JSON.stringify({
            type: 'task_started',
            taskId: taskId
          }));
          
          console.log(`✅ User ${data.userId} started task: ${taskId} - ${task.stats.totalCookies} cookies`);
          
          // Send initial stats
          ws.send(JSON.stringify({
            type: 'stats_update',
            ...task.getDetails()
          }));
          
          broadcastUserTasks(data.userId);
          saveTasksToStorage();
        }
      }
      
      // Stop task
      else if (data.type === 'stop' && ws.userId) {
        const user = userTasks.get(data.userId);
        if (user) {
          const task = user.getTask(data.taskId);
          if (task) {
            const stopped = task.stop();
            if (stopped) {
              user.removeTask(data.taskId);
              ws.send(JSON.stringify({
                type: 'task_stopped',
                taskId: data.taskId
              }));
              console.log(`🛑 User ${data.userId} stopped task: ${data.taskId}`);
              broadcastUserTasks(data.userId);
              saveTasksToStorage();
            }
          } else {
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Task not found'
            }));
          }
        }
      }
      
      // Stop all tasks
      else if (data.type === 'stop_all' && ws.userId) {
        const user = userTasks.get(data.userId);
        if (user) {
          user.stopAllTasks();
          ws.send(JSON.stringify({
            type: 'task_stopped',
            taskId: 'all'
          }));
          console.log(`🛑 User ${data.userId} stopped all tasks`);
          broadcastUserTasks(data.userId);
          saveTasksToStorage();
        }
      }
      
      // View task details
      else if (data.type === 'view_details' && ws.userId) {
        const user = userTasks.get(data.userId);
        if (user) {
          const task = user.getTask(data.taskId);
          if (task) {
            ws.send(JSON.stringify({
              type: 'task_details',
              ...task.getDetails()
            }));
          } else {
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Task not found'
            }));
          }
        }
      }
      
      // Logout
      else if (data.type === 'logout' && ws.userId) {
        const user = userTasks.get(data.userId);
        if (user) {
          user.stopAllTasks();
          userTasks.delete(data.userId);
        }
        userSessions.delete(data.userId);
        console.log(`👋 User logged out: ${data.userId}`);
        saveTasksToStorage();
      }
      
    } catch (err) {
      console.error('Error processing WebSocket message:', err);
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'error',
          message: 'Invalid request'
        }));
      }
    }
  });

  ws.on('close', () => {
    if (ws.userId) {
      const user = userTasks.get(ws.userId);
      if (user && !user.healthCheck()) {
        user.stopAllTasks();
        userTasks.delete(ws.userId);
        userSessions.delete(ws.userId);
        console.log(`👋 User disconnected: ${ws.userId}`);
        saveTasksToStorage();
      }
    }
  });
  
  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });
});

// WebSocket ping/pong for connection health
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) {
      ws.terminate();
      return;
    }
    
    ws.isAlive = false;
    try {
      ws.ping();
    } catch (e) {
      // Connection already closed
    }
  });
}, 30000);

wss.on('close', () => {
  clearInterval(pingInterval);
});

// Auto-restart system
function setupAutoRestart() {
  setInterval(() => {
    for (let [userId, user] of userTasks.entries()) {
      const tasks = user.getAllTasks();
      tasks.forEach(task => {
        if (task.config.running && !task.healthCheck()) {
          console.log(`🔄 User ${userId} - Auto-restarting task: ${task.taskId}`);
          task.restart();
        }
      });
    }
  }, 60000);
}

setupAutoRestart();

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('🛑 Shutting down gracefully...');
  
  // Save all tasks before shutdown
  saveTasksToStorage();
  
  // Stop all user tasks
  userTasks.forEach(user => user.stopAllTasks());
  userTasks.clear();
  userSessions.clear();
  
  if (consoleClearInterval) clearInterval(consoleClearInterval);
  clearInterval(pingInterval);
  
  wss.close(() => {
    console.log('👋 WebSocket server closed');
    process.exit(0);
  });
});

process.on('SIGTERM', () => {
  console.log('🛑 Terminating gracefully...');
  
  // Save all tasks before shutdown
  saveTasksToStorage();
  
  // Stop all user tasks
  userTasks.forEach(user => user.stopAllTasks());
  userTasks.clear();
  userSessions.clear();
  
  if (consoleClearInterval) clearInterval(consoleClearInterval);
  clearInterval(pingInterval);
  
  wss.close(() => {
    console.log('👋 WebSocket server closed');
    process.exit(0);
  });
});
