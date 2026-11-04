# AI / Coding-Assistant Usage

## Tools used

Arvid: Qwen 3.8 Flash Next from Alibaba locally hosted on laptop. 

Qwen from Alibaba was used to check for bugs, code improvement and assist with code development. I also used Gemini Enterprise to double check the responses from Qwen.

Fang Han: ChatGPT and Gemini for bug checking

Timothy: Claude Code was used to check for bugs, code improvement and assist with code development.

## One useful suggestion

Describe one suggestion that materially helped the project and how you verified it.

## One suggestion that was wrong, inefficient, or required modification

The AI (Gemini) was incapable of understanding the distinction between the history window maintained for late-connect/reconnects vs the per-client queue that is maintained for each active client. According to the assignment brief, late clients / reconnecting clients when requesting the caption history only require the final versions of the captions. The AI kept suggesting that it is incorrect to discard non-final revisions in the history window.

## Responsibility statement

We understand that the team remains responsible for the submitted code, experimental results, and claims, and that every member may be asked to explain or modify relevant code during the viva. The project team assumes sole risk and liability for the integration, compilation, and execution of any AI-generated assets. Under no circumstances shall the model developers, framework maintainers, or hosting infrastructure providers be held liable for any direct, indirect, incidental, consequential, or performance-related damages, system failures, runtime errors, or academic/operational penalties arising from the use or misapplication of synthesized code within this project.
