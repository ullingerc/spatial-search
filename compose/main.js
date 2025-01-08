/*
Graphical User Interface to the 'compose_spatial.py' Program

Copyright (c) 2024 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

const $ = document.querySelector.bind(document);
const $$ = document.querySelectorAll.bind(document);
const _json = url => fetch(url).then(r => r.json());
const _id = x => x;

let main = $("#main");
let output = $("#output");
let input = $("#input");

let structure = {};
let descriptions = {};
let selects = {};
let defaults = {};
let all_configs = [];

let default_config_name = "blank_compose.json";
let current_config = {};

let i_inp, openel, openall, collapseall, out_json, json_dl_btn, json_dl,
    json_pre, out_sparql, rq_dl_btn, rq_dl, rq_pre;

const createItem = (key, name, description, small = false) => {
    let el = document.createElement(small ? "div" : "details");
    el.classList.add(small ? "sitem" : "item");
    if (key)
        el.dataset.key = key;
    let tag = small ? "div" : "summary";
    // This is ok because we can trust the config in name + description
    el.innerHTML = `<${tag}><b>${name}</b> <em>${description}</em></${tag}>`;
    return el;
};

const createListItem = (el) => {
    el.classList.add("listparent")

    let list = document.createElement("ol");
    el.append(list);

    let btn = document.createElement("button");
    btn.innerText = "+ Add";
    btn.classList.add("add");
    el.appendChild(btn);
    return [el, list, btn];
};

const createSelect = (options, selected = null) => {
    let el = document.createElement("select");
    for (let [option, description] of options) {
        let o_el = document.createElement("option");
        if (description)
            o_el.innerText = option + " - " + description;
        else
            o_el.innerText = option;
        o_el.value = option;
        if (option === selected)
            o_el.selected = true;
        el.appendChild(o_el);
    }
    return el;
};

const createListElItem = (name, description) => {
    let el = document.createElement("li");

    let c = createItem(null, name, description);
    el.appendChild(c);

    let del = document.createElement("button");
    del.classList.add("del");
    del.innerHTML = "&times; Delete";
    c.appendChild(del);
    del.onclick = () => el.remove();

    return [el, c];
};

const createOutputItem = (name, description) => {
    let outel = createItem(null, name, description);
    output.appendChild(outel);
    let btn = document.createElement("button");
    outel.appendChild(btn);
    btn.style.display = "none";
    let dl = document.createElement("a");
    btn.appendChild(dl);
    dl.innerText = "Download";
    dl.href = "blob:";
    let pre = document.createElement("pre");
    outel.appendChild(pre);
    pre.innerText = "Please click 'Compose Query' first.";
    return [outel, btn, dl, pre];
};

const isObj = x => typeof x === 'object' && !Array.isArray(x) && x !== null;

const pathToString = (path, k = null) => {
    let pinterm = path.map(x => x?.dataset?.key || "_");
    if (k) pinterm.push(k);
    return pinterm.join(":");
};

const desc = (path, k = null) => {
    let pstr = pathToString(path, k);
    return descriptions?.[pstr] || [pstr, "?"];
};

const loadUIConfig = async () => {
    structure = await _json("/structure.json");
    descriptions = await _json("/descriptions.json");
    selects = await _json("/selects.json");
    defaults = await _json("/defaults.json");
    all_configs = await _json("/configs.json");
    default_config_name = await fetch("/default_input").then(r => r.text());
    current_config = await _json("/" + default_config_name);
};

const createOutputItems = () => {
    [out_json, json_dl_btn, json_dl, json_pre] = createOutputItem(
        "Output JSON", "The compose spatial config json for the current settings.");
    [out_sparql, rq_dl_btn, rq_dl, rq_pre] = createOutputItem(
        "Output SPARQL", "The result SPARQL query.");
}


const allInputsToConfigObject = () => {
    let res = {};

    // Add the values of all input elements to the configuration object
    for (let el of $$("#main [data-key] select, #main [data-key] input")) {
        // Get the value of this input field
        let val = el.value;
        if (el.tagName == "INPUT" && el.type == "number") {
            val = Number(val);
        }

        // For each element, go up the DOM tree to find its path of keys in the config object
        let path = [], currentEl = el, n = null;
        while (currentEl != main) {
            if (currentEl.dataset.key) {
                path.push([n, currentEl.dataset.key]);
                n = null;
            }
            // Special treatment for lists
            if (currentEl.tagName == "LI") {
                var i = 0;
                let child = currentEl;
                // Find out what n-th child of the list we are looking at
                // Quadratic, but there are no large lists, so it's ok
                while ((child = child.previousSibling) != null)
                    i++;
                n = i;
            }
            // Go up one level in DOM tree
            currentEl = currentEl.parentElement;
        }

        // Path is child-first, we want parent-first
        path.reverse();

        // Go down the path again to assemble the nested output object
        let currentRes = res;
        if (n != null) {
            path[path.length - 1][0] = n;
        }
        let [pindex, last] = path.pop();
        for (let [pindex, psegment] of path) {
            if (pindex != null) {
                // List
                if (!currentRes[psegment]) {
                    currentRes[psegment] = [];
                }
                if (!currentRes[psegment][pindex]) {
                    currentRes[psegment][pindex] = {};
                }
                currentRes = currentRes[psegment][pindex];
            } else {
                // Object
                if (!currentRes[psegment]) {
                    currentRes[psegment] = {};
                }
                currentRes = currentRes[psegment];
            }
        }

        // Now add the value to the result object
        if (pindex === null) {
            currentRes[last] = val || null;
        } else {
            if (!currentRes[last]) {
                currentRes[last] = [];
            }
            currentRes[last][pindex] = val || null;
        }
    }
    return res;
};

const createItemsRecursively = (path, current, item, data) => {
    // From a subtree of our config object, create the necessary GUI elements

    if (isObj(item)) {
        // Create a GUI subtree for every key in the object 
        for (let k of Object.keys(item)) {
            // Inputs fields for numbers/strings should be small
            let small = (typeof item[k] == "string" || typeof item[k] == "number");

            // Description and HTML element
            let [dName, dLong] = desc(path, k);
            let el = createItem(k, dName, dLong, small);
            current.append(el);

            // Create the children of this element
            createItemsRecursively([...path, el], el, item[k], data?.[k]);
        }
    } else if (Array.isArray(item)) {
        // Create a list subtree with numbered items and "Add" button
        let [_, list, add_btn] = createListItem(current);
        item = item[0];

        // If the members of the list are only a string or number input,
        // automatically expand the folded <details> tags 
        let small = (typeof item == "string" || typeof item == "number");

        let i = 0;

        // Helper function to create a list entry with its children (optionally
        // given data) -> this is used to initialize the list using data but
        // also as an action for the "Add" button. Thanks to JS inheriting the
        // context and preserving it from garbage collection this avoids lots
        // of annoying code.
        const addListEntry = (data_ = null) => {
            // Create and append the new list entry
            let [dName, dLong] = desc(path, "_");
            let [listEntryEl, childDetailsEl] = createListElItem(dName, dLong);
            list.appendChild(listEntryEl);
            childDetailsEl.open = small;

            // Create the list entry's children using captured context
            createItemsRecursively([...path, i], childDetailsEl, item, data_);
            i++;
        };

        // Event listener for "Add" button
        let defaultData = defaults?.[pathToString(path)];
        add_btn.onclick = () => addListEntry(defaultData);

        // Add informative label to "Add" button
        let [dName,] = desc(path, "_");
        if (dName) {
            add_btn.innerText += " " + dName;
        }

        // If the list has entries in the input data: create all list entry
        // elements
        if (data)
            for (let k of data) {
                addListEntry(k);
            }

    } else if (typeof item == "string") {
        // Create a "terminal" string input field

        // A string input can be either free-form text or a <select> tag
        // It is a <select> iff we have a list of options given
        let select = selects?.[pathToString(path)];
        if (select) {
            // Default value
            let val = select[1];
            if (data) {
                val = data;
                // If the value in the current data is not one of the defined
                // options: add it
                if (select[0].map(opt => opt[0]).indexOf(data) < 0) {
                    select[0].push([data, ""]);
                }
            };

            // Create and append <select> tag with <option> children
            let el = createSelect(select[0], val);
            current.appendChild(el);

            // If the options are SPARQL query files, offer to view the
            // currently selected one
            if (select[0].every(opt => opt[0].endsWith(".rq") ||
                opt[0].endsWith(".sparql") || opt[0] === "")) {
                let viewEl = document.createElement("button");
                viewEl.innerText = "Preview";
                viewEl.classList.add("view_btn");

                viewEl.onclick = async () => {
                    if (!el.value) {
                        return;
                    }

                    let content = await fetch(el.value).then(
                        res => res.text());

                    // Open a dialog window with the file's content
                    let dialog = document.createElement("dialog");

                    let dialogHead = document.createElement("h2");
                    dialogHead.innerText = el.value;
                    dialog.appendChild(dialogHead);

                    let dialogClose = document.createElement("button");
                    dialogHead.appendChild(dialogClose);
                    dialogClose.innerHTML = "&times; Close";
                    dialogClose.onclick = () => {
                        dialog.close();
                        dialog.remove();
                    };
                    dialogClose.classList.add("dialog_close");

                    let dialogBody = document.createElement("pre");
                    dialog.appendChild(dialogBody);
                    dialogBody.innerText = content;

                    current.appendChild(dialog);
                    dialog.showModal();
                };
                current.appendChild(viewEl);
            }
        } else {
            // Free form text input
            let el = document.createElement("input");
            el.type = "text";
            current.appendChild(el);

            if (data) {
                el.value = data;
            } else {
                let defaultValue = defaults?.[pathToString(path)];
                if (defaultValue)
                    el.value = defaultValue;
            }
        }
    } else if (typeof item == "number") {
        // Create a "terminal" numeric input field
        let el = document.createElement("input");
        el.type = "number";
        el.min = 0;
        current.appendChild(el);

        if (data !== null && typeof data != "undefined") {
            el.value = data || 0;
        } else {
            let defaultValue = defaults?.[pathToString(path)];
            el.value = defaultValue || 0;
        }
    }
};

const loadDocStructureFromObject = (data = null) => createItemsRecursively([], main, structure, data);

const toggleAllDetailsElements = (open = true) => {
    for (let el of $$("details"))
        el.open = open;
};

const createOpenAll = () => {
    openall = document.createElement("button");
    openall.innerText = "Expand all"
    openel.appendChild(openall);
    openall.classList.add("expbtn");
    openall.onclick = () => toggleAllDetailsElements(true);
};

const createCollapseAll = () => {
    collapseall = document.createElement("button");
    collapseall.innerText = "Collapse all";
    openel.appendChild(collapseall);
    collapseall.classList.add("expbtn");
    collapseall.onclick = () => toggleAllDetailsElements(false);
};

const generateConfig = () => {
    let result = allInputsToConfigObject();

    let res_json = JSON.stringify(result, null, 2);
    json_pre.innerText = res_json;
    json_dl.href = URL.createObjectURL(new Blob([res_json], {
        type: "application/json"
    }));

    let json_fn = "compose_spatial.json";
    if (i_inp.value != "blank_compose.json")
        json_fn = i_inp.value;
    json_dl.download = json_fn;
    json_dl.innerText = "Download " + json_fn;
    json_dl_btn.style.display = "";

    return res_json;
};

const composeQueryRequest = async res_json => {
    return fetch("/compose", {
        method: "POST",
        headers: {
            'Accept': 'application/sparql-query',
            'Content-Type': 'application/json'
        },
        body: res_json
    }).then(async r => [r.status, await r.text()]).catch(_id);
};

const compose = async () => {
    rq_pre.classList.remove("error");
    rq_dl_btn.style.display = "";
    let res_json = generateConfig();
    let res = await composeQueryRequest(res_json);
    let ok = (res?.[0] == 200);
    let s = res?.[1];
    if (!ok) {
        rq_pre.classList.add("error");
        rq_dl_btn.style.display = "none";
    }

    rq_pre.innerText = s;
    out_sparql.open = true;
    rq_dl.href = URL.createObjectURL(new Blob([s], {
        type: "application/sparql-query"
    }));
    let rq_fn = "compose_spatial.rq";
    if (i_inp.value != "blank_compose.json")
        rq_fn = i_inp.value.replace("_compose", "").replace(".json", ".rq");
    rq_dl.download = rq_fn;
    rq_dl.innerText = "Download " + rq_fn;
}

$("#compose").onclick = compose;

const createOpenItem = () => {
    openel = createItem(null, "Open", "Load a predefined configuration.", true);
    input.appendChild(openel);
    i_inp = createSelect(all_configs, default_config_name);
    openel.appendChild(i_inp);
    i_inp.onchange = async () => {
        main.innerHTML = "Loading...";
        let data = await fetch("/" + i_inp.value).then(r => r.json());
        main.innerHTML = "";
        loadDocStructureFromObject(data);
        output.innerHTML = "";
        createOutputItems();
    };
    createOpenAll();
    createCollapseAll();
};

(async () => {
    await loadUIConfig();
    loadDocStructureFromObject(current_config);
    createOpenItem();
    createOutputItems();
})();
