
let selectedFiles = [];
function displaySelectedFiles(input) {
    const newFiles = Array.from(input.files);
    const uniqueFiles = newFiles.filter(newFile => {
        return !selectedFiles.some(existingFile => existingFile.name === newFile.name && existingFile.size === newFile.size);
    });
    selectedFiles = selectedFiles.concat(uniqueFiles);
    input.value = ''; // Reset input value for re-upload
    updateFileList();
}
function updateFileList() {
    const fileList = document.getElementById('file-list');
    fileList.innerHTML = ''; // Clear previous list
    selectedFiles.forEach((file, index) => {
        const listItem = document.createElement('div');
        listItem.className = 'file-item';
        listItem.textContent = file.name;
        const removeButton = document.createElement('button');
        removeButton.textContent = 'Remove';
        removeButton.type = 'button';
        removeButton.onclick = () => removeFile(index);
        listItem.appendChild(removeButton);
        fileList.appendChild(listItem);
    });
    document.getElementById('file-label').textContent = `${selectedFiles.length} file(s) selected`;
}
function removeFile(index) {
    selectedFiles.splice(index, 1);
    updateFileList();
}
function submitForm(event) {
    event.preventDefault();
    const formData = new FormData();
    selectedFiles.forEach(file => formData.append("csv_files[]", file));
    fetch('upload.php', {
        method: 'POST',
        body: formData
    }).then(() => alert("Files uploaded and processed successfully."))
        .catch(error => console.error('Error:', error));
}