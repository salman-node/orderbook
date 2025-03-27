const userID = 10; // Example pair ID (change as needed)
const ws = new WebSocket("ws://192.168.0.46:8081"); // Replace with actual server IP/hostname

ws.onopen = () => {
  console.log("Connected to WebSocket server");

  // Subscribe to updates for the given pairId
  ws.send(JSON.stringify({ userID }));
};

ws.onmessage = (event) => {
  try {
    const data = JSON.parse(event.data);
    // console.log("Received update:", data);

    console.log("Received update:", data);  
  } catch (error) {
    console.error("Error parsing message:", error);
  }
};

ws.onerror = (error) => {
  console.error("WebSocket error:", error);
};

ws.onclose = () => {
  console.log("Disconnected from WebSocket server");
};