let selectedFiles = [];

function displaySelectedFiles(input) {
	const newFiles = Array.from(input.files);
	const uniqueFiles = newFiles.filter((newFile) => {
		return !selectedFiles.some(
			(existingFile) =>
				existingFile.name === newFile.name &&
				existingFile.size === newFile.size
		);
	});
	selectedFiles = selectedFiles.concat(uniqueFiles);
	input.value = ""; // Reset input value for re-upload
	updateFileList();
}

function updateFileList() {
	const fileList = document.getElementById("file-list");
	fileList.innerHTML = ""; // Clear previous list
	selectedFiles.forEach((file, index) => {
		const listItem = document.createElement("div");
		listItem.className = "file-item";
		listItem.textContent = file.name;
		const removeButton = document.createElement("button");
		removeButton.textContent = "Remove";
		removeButton.type = "button";
		removeButton.onclick = () => removeFile(index);
		listItem.appendChild(removeButton);
		fileList.appendChild(listItem);
	});
}

function removeFile(index) {
	selectedFiles.splice(index, 1);
	updateFileList();
}

function submitForm(event) {
	event.preventDefault();
	const formData = new FormData();
	selectedFiles.forEach((file) => formData.append("csv_files[]", file));
	document.getElementById("loading-screen").style.display = "flex"; // Loading screen starts here
	fetch("upload.php", {
		method: "POST",
		body: formData,
	})
		.then((response) => response.json()) // Parse the JSON response from the server
		.then((data) => {
			document.getElementById("loading-screen").style.display = "none"; // Loading screen ends here on success
			alert(data.message); // Display the response message in a popup
			if (
				data.message.includes(
					"Files uploaded and processed successfully!"
				)
			) {
				// Log the message to message.log
				fetch("log_message.php", {
					method: "POST",
					headers: {
						"Content-Type": "application/json",
					},
					body: JSON.stringify({ message: data.message }),
				});
			}
		})
		.catch((error) => {
			document.getElementById("loading-screen").style.display = "none"; // Loading screen ends here on error
			console.error("Error:", error);
		});
}
